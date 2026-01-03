import json
import re
import sqlite3
import threading
import time
from typing import Any, Dict, List, Optional, Tuple, Union

from .settings import get_db_path
from . import jsbridge
from .tag_updater import OutboundTagUpdater, TagUpdaterStopped


class JsonUpdaterStopped(Exception):
    """Signal برای توقف graceful JSON updater."""


class JsonConfigUpdater:
    """
    ساخت config_json در جدول links (Batch=1000).
    این فایل خودش ستون‌های اختیاری is_invalid و is_protocol_unsupported را (اگر نبودند) اضافه می‌کند
    تا query به خطا نخورد و بتواند ردیف‌های خراب/unsupported را اسکیپ کند.

    تغییر جدید:
      - قبل از شروع کار، tag_updater اجرا می‌شود تا outbound_tag برای همه رکوردهای بدون تگ پر شود.
      - خروجی JSON حتماً tag را از روی ستون outbound_tag جایگزین/ست می‌کند.
    """

    SUPPORTED_PROTOCOLS = {
        "vless",
        "vmess",
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
            "marked_invalid": 0,
            "marked_unsupported": 0,
        }

        self._stop_event = threading.Event()
        self._stats_lock = threading.Lock()
        self._tag_updater: Optional[OutboundTagUpdater] = None

    # ---------- stop ----------
    def request_stop(self) -> None:
        print("[json_updater] stop requested")
        self._stop_event.set()
        try:
            if self._tag_updater is not None:
                self._tag_updater.request_stop()
        except Exception:
            pass
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
        s = (url or "").strip()
        if not s:
            return None
        m = re.match(r"^([a-zA-Z0-9\-\+]+)://", s)
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
    def _apply_outbound_tag(outbound: Any, outbound_tag: str) -> Any:
        """
        خروجی bridge ممکن است dict/list یا یک string JSON باشد.
        این تابع tag را با مقدار outbound_tag جایگزین/ست می‌کند و آبجکت نهایی را برمی‌گرداند.
        """
        tag = (outbound_tag or "").strip()
        if not tag:
            raise ValueError("empty outbound_tag")

        obj: Any = outbound
        if isinstance(obj, str):
            s = obj.strip()
            if not s:
                raise ValueError("empty JSON string from bridge")
            try:
                obj = json.loads(s)
            except Exception as e:
                raise ValueError(f"bridge returned non-JSON string: {e}") from e

        if isinstance(obj, dict):
            obj["tag"] = tag
            return obj

        # در برخی buildها ممکن است bridge لیست برگرداند؛ اگر تک‌آیتمی و dict باشد قابل تحمل است.
        if isinstance(obj, list) and len(obj) == 1 and isinstance(obj[0], dict):
            obj[0]["tag"] = tag
            return obj

        raise ValueError(f"unexpected outbound type for tag injection: {type(obj).__name__}")

    @staticmethod
    def _table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
        cur = conn.cursor()
        cur.execute(f"PRAGMA table_info({table})")
        return [str(r[1]) for r in cur.fetchall()]

    def _ensure_optional_columns(self, conn: sqlite3.Connection) -> None:
        cols = set(self._table_columns(conn, "links"))
        alters: List[str] = []

        if "is_invalid" not in cols:
            alters.append("ALTER TABLE links ADD COLUMN is_invalid INTEGER NOT NULL DEFAULT 0")
        if "is_protocol_unsupported" not in cols:
            alters.append("ALTER TABLE links ADD COLUMN is_protocol_unsupported INTEGER NOT NULL DEFAULT 0")
        if "repaired_url" not in cols:
            alters.append("ALTER TABLE links ADD COLUMN repaired_url TEXT")

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
        except Exception as e:
            msg = str(e)
            # retry روی برخی خطاهای transient
            if "timeout" in msg.lower() or "not ready" in msg.lower():
                time.sleep(0.2)
                return self._convert_url_to_outbound(url)
            raise

    # ---------- main ----------
    def update_missing_json(self) -> None:
        print(f"[json_updater] starting JSON update job (batch_size={self.batch_size})")

        self._check_stopped()
        # 0) اطمینان از وجود/تکمیل outbound_tag قبل از ساخت JSON
        try:
            self._tag_updater = OutboundTagUpdater(batch_size=self.batch_size)
            self._tag_updater.run()
        except TagUpdaterStopped:
            raise JsonUpdaterStopped()
        finally:
            self._tag_updater = None

        self._check_stopped()

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
            has_outbound_tag = "outbound_tag" in cols

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
                if has_outbound_tag:
                    where_parts.append("outbound_tag IS NOT NULL")
                    where_parts.append("TRIM(outbound_tag) <> ''")

                sql = (
                    "SELECT id, url, outbound_tag FROM links "
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

                updates_json: List[Tuple[str, int]] = []
                mark_invalid: List[int] = []
                mark_unsupported: List[int] = []

                for row in rows:
                    self._check_stopped()

                    row_id = int(row["id"])
                    last_id = row_id

                    outbound_tag_val = row["outbound_tag"] if has_outbound_tag else None
                    outbound_tag = str(outbound_tag_val).strip() if outbound_tag_val is not None else ""

                    url_val = row["url"]
                    if url_val is None:
                        mark_invalid.append(row_id)
                        continue

                    url_str = str(url_val).strip()
                    if not url_str:
                        mark_invalid.append(row_id)
                        continue

                    with self._stats_lock:
                        self.stats["urls_seen"] += 1

                    proto = self._detect_protocol(url_str)
                    if proto and proto not in self.SUPPORTED_PROTOCOLS:
                        mark_unsupported.append(row_id)
                        continue

                    try:
                        outbound = self._convert_with_one_retry(url_str)
                        outbound = self._apply_outbound_tag(outbound, outbound_tag) if has_outbound_tag else outbound
                        json_text = self._canonical_json(outbound)
                        if not json_text:
                            raise ValueError("empty JSON from bridge")
                        updates_json.append((json_text, row_id))
                        with self._stats_lock:
                            self.stats["urls_converted"] += 1
                    except Exception as e:
                        print(f"[json_updater] convert failed id={row_id}: {e}")
                        mark_invalid.append(row_id)
                        with self._stats_lock:
                            self.stats["urls_failed"] += 1

                # apply updates
                cur = conn.cursor()
                cur.execute("BEGIN")
                try:
                    if updates_json:
                        cur.executemany(
                            "UPDATE links SET config_json = ? WHERE id = ?",
                            updates_json,
                        )

                    if mark_invalid and has_is_invalid:
                        cur.executemany(
                            "UPDATE links SET is_invalid = 1 WHERE id = ?",
                            [(i,) for i in mark_invalid],
                        )

                    if mark_unsupported and has_unsupported:
                        cur.executemany(
                            "UPDATE links SET is_protocol_unsupported = 1, is_invalid = 0 WHERE id = ?",
                            [(i,) for i in mark_unsupported],
                        )

                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise

                with self._stats_lock:
                    if mark_invalid and has_is_invalid:
                        self.stats["marked_invalid"] += len(mark_invalid)
                    if mark_unsupported and has_unsupported:
                        self.stats["marked_unsupported"] += len(mark_unsupported)

                print(
                    f"[json_updater] batch done: {batch_size} rows, "
                    f"converted={len(updates_json)}, invalid={len(mark_invalid)}, unsupported={len(mark_unsupported)}"
                )

        except JsonUpdaterStopped:
            print("[json_updater] stopped by request.")
        finally:
            try:
                conn.close()
            except Exception:
                pass

        print(
            "[json_updater] finished:"
            f"\n total_candidates: {self.stats['total_candidates']}"
            f"\n batches: {self.stats['batches']}"
            f"\n urls_seen: {self.stats['urls_seen']}"
            f"\n urls_converted: {self.stats['urls_converted']}"
            f"\n urls_failed: {self.stats['urls_failed']}"
            f"\n marked_invalid: {self.stats['marked_invalid']}"
            f"\n marked_unsupported: {self.stats['marked_unsupported']}"
        )


if __name__ == "__main__":
    JsonConfigUpdater(batch_size=1000).update_missing_json()
