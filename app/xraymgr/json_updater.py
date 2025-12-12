import json
import re
import sqlite3
import threading
from typing import Any, Dict, List, Optional, Tuple, Union

from .settings import get_db_path
from . import jsbridge


class JsonUpdaterStopped(Exception):
    """Signal برای توقف graceful JSON updater."""


class JsonConfigUpdater:
    """
    پر کردن ستون config_json در جدول links برای کانفیگ‌هایی که:
      - config_json IS NULL یا خالی است
      - url غیرخالی است
      - (در صورت وجود ستون‌ها) is_invalid = 0 و is_protocol_unsupported = 0

    منطق پروتکل:
      - فقط پروتکل‌های زیر فعلاً ساپورت می‌شوند:
        vmess / vless / trojan / ss / shadowsocks / shadowsocks2022
      - هر URL که پروتکل دیگری داشته باشد → is_protocol_unsupported = 1 (اگر ستون وجود داشته باشد)
      - برای پروتکل‌های ساپورت‌شده، از jsbridge برای تبدیل به outbound JSON استفاده می‌شود.
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
            "rows_marked_invalid": 0,
            "rows_marked_unsupported": 0,
        }

        self._stop_event = threading.Event()
        self._stats_lock = threading.Lock()

    # ---------- کنترل توقف ----------
    def request_stop(self) -> None:
        print("[json_updater] stop requested")
        self._stop_event.set()

    def _check_stopped(self) -> None:
        if self._stop_event.is_set():
            raise JsonUpdaterStopped()

    # ---------- schema helpers ----------
    @staticmethod
    def _table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
        cur = conn.cursor()
        cur.execute(f"PRAGMA table_info({table})")
        return [str(r[1]) for r in cur.fetchall()]  # (cid, name, type, notnull, dflt, pk)

    @classmethod
    def _ensure_links_columns(cls, conn: sqlite3.Connection) -> None:
        """
        برای پایدار کردن job روی DBهای قدیمی‌تر:
        اگر ستون‌های is_invalid / is_protocol_unsupported وجود نداشته باشند، اضافه‌شان می‌کند.
        """
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

    # ---------- ابزارهای کمکی ----------
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

    def _convert_url_to_outbound(self, url: str) -> Union[Dict[str, Any], Any, str]:
        return jsbridge.convert_to_outbound(url, timeout=self.node_timeout)

    # ---------- منطق اصلی ----------
    def update_missing_json(self) -> None:
        print(f"[json_updater] starting JSON update job (batch_size={self.batch_size})")

        conn = sqlite3.connect(get_db_path())
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute("PRAGMA temp_store = MEMORY;")

        try:
            try:
                self._ensure_links_columns(conn)
            except Exception as e:
                print(f"[json_updater] WARN: could not ensure schema columns: {e}")

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
                        print(f"[json_updater] no protocol detected for id={row_id}, url={url_str!r}")
                        mark_invalid.append(row_id)
                        with self._stats_lock:
                            self.stats["urls_failed"] += 1
                        continue

                    if proto not in self.SUPPORTED_PROTOCOLS:
                        print(f"[json_updater] unsupported protocol {proto!r} for id={row_id}")
                        mark_unsupported.append(row_id)
                        with self._stats_lock:
                            self.stats["urls_failed"] += 1
                        continue

                    try:
                        outbound = self._convert_url_to_outbound(url_str)
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
                cur = conn.cursor()

                if updates_json:
                    try:
                        cur.execute("BEGIN")
                        cur.executemany("UPDATE links SET config_json = ? WHERE id = ?", updates_json)
                        conn.commit()
                        rows_changed += cur.rowcount
                        print(
                            f"[json_updater] updated config_json for {len(updates_json)} rows "
                            f"(sqlite rowcount={cur.rowcount})"
                        )
                    except Exception as e:
                        conn.rollback()
                        print(f"[json_updater] ERROR committing config_json updates: {e}")

                if mark_invalid:
                    try:
                        if has_is_invalid:
                            cur.execute("BEGIN")
                            cur.executemany(
                                "UPDATE links SET is_invalid = 1 WHERE id = ?",
                                [(rid,) for rid in mark_invalid],
                            )
                            conn.commit()
                            with self._stats_lock:
                                self.stats["rows_marked_invalid"] += len(mark_invalid)
                            rows_changed += cur.rowcount
                        else:
                            cur.execute("BEGIN")
                            cur.executemany(
                                "UPDATE links SET needs_replace = 1, is_alive = 0 WHERE id = ?",
                                [(rid,) for rid in mark_invalid],
                            )
                            conn.commit()
                            rows_changed += cur.rowcount
                        print(
                            f"[json_updater] marked {len(mark_invalid)} rows as invalid "
                            f"(sqlite rowcount={cur.rowcount})"
                        )
                    except Exception as e:
                        conn.rollback()
                        print(f"[json_updater] ERROR marking invalid rows: {e}")

                if mark_unsupported:
                    try:
                        if has_unsupported:
                            cur.execute("BEGIN")
                            cur.executemany(
                                "UPDATE links SET is_protocol_unsupported = 1 WHERE id = ?",
                                [(rid,) for rid in mark_unsupported],
                            )
                            conn.commit()
                            with self._stats_lock:
                                self.stats["rows_marked_unsupported"] += len(mark_unsupported)
                            rows_changed += cur.rowcount
                        else:
                            cur.execute("BEGIN")
                            cur.executemany(
                                "UPDATE links SET needs_replace = 1 WHERE id = ?",
                                [(rid,) for rid in mark_unsupported],
                            )
                            conn.commit()
                            rows_changed += cur.rowcount
                        print(
                            f"[json_updater] marked {len(mark_unsupported)} rows as protocol_unsupported "
                            f"(sqlite rowcount={cur.rowcount})"
                        )
                    except Exception as e:
                        conn.rollback()
                        print(f"[json_updater] ERROR marking unsupported rows: {e}")

                if rows_changed:
                    with self._stats_lock:
                        self.stats["rows_updated"] += rows_changed

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
            f"\n rows marked invalid: {self.stats['rows_marked_invalid']}"
            f"\n rows marked unsupported: {self.stats['rows_marked_unsupported']}"
        )


if __name__ == "__main__":
    updater = JsonConfigUpdater(batch_size=1000)
    updater.update_missing_json()
