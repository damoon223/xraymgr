import json
import re
import sqlite3
import threading
import time
from typing import Any, Dict, List, Optional, Tuple, Union

from .settings import get_db_path
from . import jsbridge


class JsonUpdaterStopped(Exception):
    """Signal برای توقف graceful JSON updater."""


class JsonConfigUpdater:
    """
    ساخت config_json در جدول links (Batch=1000).
    این فایل خودش ستون‌های اختیاری is_invalid و is_protocol_unsupported را (اگر نبودند) اضافه می‌کند
    تا query به خطا نخورد و بتواند ردیف‌های خراب/unsupported را اسکیپ کند.
    """

    SUPPORTED_PROTOCOLS = {
        "vmess",
        "vless",
        "trojan",
        "ss",
        "shadowsocks",
        "shadowsocks2022",
    }

    def __init__(self, batch_size: int = 1000, node_timeout: float = jsbridge.DEFAULT_NODE_TIMEOUT) -> None:
        self.batch_size = int(batch_size)
        self.node_timeout = float(node_timeout)

        self.stats: Dict[str, int] = {
            "total_candidates": 0,
            "batches": 0,
            "urls_seen": 0,
            "urls_converted": 0,
            "urls_failed": 0,
            "rows_updated": 0,
        }

        self._stop_event = threading.Event()
        self._stats_lock = threading.Lock()

    # ---------- stop ----------
    def request_stop(self) -> None:
        print("[json_updater] stop requested")
        self._stop_event.set()
        # اگر وسط call به node گیر کند، این کمک می‌کند سریع‌تر آزاد شود
        try:
            jsbridge.close_global_converter()
        except Exception:
            pass

    def _check_stopped(self) -> None:
        if self._stop_event.is_set():
            raise JsonUpdaterStopped()

    # ---------- helpers ----------
    @staticmethod
    def _detect_protocol(url: str) -> Optional[str]:
        if not url:
            return None
        url_str = str(url).strip()
        m = re.match(r"^([a-zA-Z0-9+.\-]+)://", url_str)
        if not m:
            return None
        proto = m.group(1).strip().lower()
        return proto or None

    @staticmethod
    def _canonical_json(obj: Any) -> str:
        if isinstance(obj, str):
            return obj.strip()
        if isinstance(obj, (dict, list)):
            return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)

    @staticmethod
    def _table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
        cur = conn.cursor()
        cur.execute(f"PRAGMA table_info({table})")
        return [str(r[1]) for r in cur.fetchall()]

    @classmethod
    def _ensure_optional_columns(cls, conn: sqlite3.Connection) -> None:
        existing = set(cls._table_columns(conn, "links"))
        alters: List[str] = []
        if "is_invalid" not in existing:
            alters.append("ALTER TABLE links ADD COLUMN is_invalid INTEGER NOT NULL DEFAULT 0")
        if "is_protocol_unsupported" not in existing:
            alters.append("ALTER TABLE links ADD COLUMN is_protocol_unsupported INTEGER NOT NULL DEFAULT 0")

        if not alters:
            return

        cur = conn.cursor()
        for stmt in alters:
            try:
                cur.execute(stmt)
            except sqlite3.Error as e:
                print(f"[json_updater] WARN: schema alter failed: {e} (stmt={stmt})")
        conn.commit()

    def _convert_url_to_outbound(self, url: str) -> Union[Dict[str, Any], Any]:
        return jsbridge.convert_to_outbound(url, timeout=self.node_timeout)

    def _convert_with_one_retry(self, url: str) -> Union[Dict[str, Any], Any]:
        try:
            return self._convert_url_to_outbound(url)
        except jsbridge.NodeBridgeError as e:
            msg = str(e)
            if "INFRA:" in msg or "READY" in msg or "init" in msg.lower() or "OUTBOUND_NOT_FOUND" in msg:
                print(f"[json_updater] INFRA error from bridge, restarting converter and retrying once: {e}")
                try:
                    jsbridge.close_global_converter()
                except Exception:
                    pass
                time.sleep(0.2)
                return self._convert_url_to_outbound(url)
            raise

    # ---------- main ----------
    def update_missing_json(self) -> None:
        print(f"[json_updater] starting JSON update job (batch_size={self.batch_size})")

        conn = sqlite3.connect(get_db_path())
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute("PRAGMA temp_store = MEMORY;")

        try:
            # ensure columns so query does not break on your current DB schema
            try:
                self._ensure_optional_columns(conn)
            except Exception as e:
                print(f"[json_updater] WARN: could not ensure optional columns: {e}")

            cols = set(self._table_columns(conn, "links"))
            has_is_invalid = "is_invalid" in cols
            has_unsupported = "is_protocol_unsupported" in cols

            last_id = 0
            while True:
                self._check_stopped()

                where_parts: List[str] = [
                    "(config_json IS NULL OR TRIM(config_json) = '')",
                    "url IS NOT NULL",
                    "TRIM(url) <> ''",
                    "id > ?",
                ]
                if has_is_invalid:
                    where_parts.append("is_invalid = 0")
                if has_unsupported:
                    where_parts.append("is_protocol_unsupported = 0")

                sql = (
                    "SELECT id, url FROM links "
                    f"WHERE {' AND '.join(where_parts)} "
                    "ORDER BY id LIMIT ?"
                )

                cur = conn.cursor()
                cur.execute(sql, (last_id, self.batch_size))
                rows = cur.fetchall()

                if not rows:
                    print("[json_updater] no more candidates, exiting loop.")
                    break

                batch_size = len(rows)
                with self._stats_lock:
                    self.stats["batches"] += 1
                    self.stats["total_candidates"] += batch_size

                print(f"[json_updater] batch loaded: {batch_size} rows (last_id before batch={last_id})")

                updates_json: List[Tuple[str, int]] = []
                mark_invalid: List[int] = []
                mark_unsupported: List[int] = []

                for row in rows:
                    self._check_stopped()

                    row_id = int(row["id"])
                    last_id = row_id

                    url_val = row["url"]
                    if url_val is None:
                        mark_invalid.append(row_id)
                        with self._stats_lock:
                            self.stats["urls_failed"] += 1
                        continue

                    url_str = str(url_val).strip()
                    if not url_str:
                        mark_invalid.append(row_id)
                        with self._stats_lock:
                            self.stats["urls_failed"] += 1
                        continue

                    with self._stats_lock:
                        self.stats["urls_seen"] += 1

                    proto = self._detect_protocol(url_str)
                    if not proto:
                        mark_invalid.append(row_id)
                        with self._stats_lock:
                            self.stats["urls_failed"] += 1
                        continue

                    if proto not in self.SUPPORTED_PROTOCOLS:
                        mark_unsupported.append(row_id)
                        with self._stats_lock:
                            self.stats["urls_failed"] += 1
                        continue

                    try:
                        outbound = self._convert_with_one_retry(url_str)
                        json_text = self._canonical_json(outbound)
                        if not json_text:
                            raise ValueError("empty JSON from bridge")
                        updates_json.append((json_text, row_id))
                        with self._stats_lock:
                            self.stats["urls_converted"] += 1
                    except Exception as e:
                        print(f"[json_updater] ERROR converting url for id={row_id}: {e}")
                        mark_invalid.append(row_id)
                        with self._stats_lock:
                            self.stats["urls_failed"] += 1

                rows_changed = 0
                try:
                    with conn:
                        cur = conn.cursor()

                        if updates_json:
                            cur.executemany("UPDATE links SET config_json = ? WHERE id = ?", updates_json)
                            rows_changed += len(updates_json)

                        if has_is_invalid and mark_invalid:
                            cur.executemany(
                                "UPDATE links SET is_invalid = 1 WHERE id = ?",
                                [(rid,) for rid in mark_invalid],
                            )
                            rows_changed += len(mark_invalid)

                        if has_unsupported and mark_unsupported:
                            cur.executemany(
                                "UPDATE links SET is_protocol_unsupported = 1 WHERE id = ?",
                                [(rid,) for rid in mark_unsupported],
                            )
                            rows_changed += len(mark_unsupported)

                    with self._stats_lock:
                        self.stats["rows_updated"] += rows_changed

                    print(
                        "[json_updater] batch committed: "
                        f"config_json={len(updates_json)}, invalid={len(mark_invalid)}, unsupported={len(mark_unsupported)}"
                    )
                except Exception as e:
                    print(f"[json_updater] ERROR committing batch: {e}")

        except JsonUpdaterStopped:
            print("[json_updater] stopped by request.")
        finally:
            try:
                conn.close()
                print("[json_updater] DB connection closed.")
            except Exception:
                pass

        print(
            "[json_updater] job finished:"
            f"\n total candidates: {self.stats['total_candidates']}"
            f"\n batches: {self.stats['batches']}"
            f"\n urls seen: {self.stats['urls_seen']}"
            f"\n urls converted: {self.stats['urls_converted']}"
            f"\n urls failed: {self.stats['urls_failed']}"
            f"\n rows updated: {self.stats['rows_updated']}"
        )


if __name__ == "__main__":
    updater = JsonConfigUpdater(batch_size=1000)
    updater.update_missing_json()
