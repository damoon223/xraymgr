import json
import re
import sqlite3
import threading
from typing import Any, Dict, List, Optional, Tuple

from .settings import get_db_path
from . import jsbridge


class JsonUpdaterStopped(Exception):
    """Signal برای توقف graceful JSON updater."""
    pass


class JsonConfigUpdater:
    """
    پر کردن ستون config_json در جدول links برای کانفیگ‌هایی که:

      - config_json IS NULL یا خالی است
      - is_invalid = 0
      - is_protocol_unsupported = 0  (آنهایی که قبلاً unsupported علامت‌گذاری شده‌اند، اسکیپ می‌شوند)

    منطق پروتکل:

      - فقط پروتکل‌های زیر فعلاً ساپورت می‌شوند:
          vmess / vless / trojan / ss / shadowsocks / shadowsocks2022
      - هر URL که پروتکل دیگری داشته باشد → is_protocol_unsupported = 1
      - برای پروتکل‌های ساپورت‌شده، از jsbridge برای تبدیل به outbound JSON استفاده می‌شود.

    stats (برای UI پنل وب):

      - total_candidates: مجموع ردیف‌هایی که از دیتابیس به عنوان کاندید لود شده‌اند
      - batches: تعداد batchهای پردازش‌شده
      - urls_seen: تعداد URLهایی که واقعاً بررسی شدند
      - urls_converted: تعداد URLهایی که موفق به تولید config_json شدند
      - urls_failed: تعداد URLهایی که شکست خوردند (خطای تبدیل یا داده خراب)
      - rows_updated: تعداد ردیف‌هایی که در DB تغییری روی آن‌ها اعمال شد
    """

    SUPPORTED_PROTOCOLS = {
        "vmess",
        "vless",
        "trojan",
        "ss",
        "shadowsocks",
        "shadowsocks2022",
    }

    def __init__(self, batch_size: int = 1000) -> None:
        self.batch_size = batch_size
        self.stats = {
            "total_candidates": 0,
            "batches": 0,
            "urls_seen": 0,
            "urls_converted": 0,
            "urls_failed": 0,
            "rows_updated": 0,
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

    # ---------- ابزارهای کمکی ----------

    @staticmethod
    def _detect_protocol(url: str) -> Optional[str]:
        """
        از ابتدای رشتهٔ URL پروتکل را تشخیص می‌دهد، مثل:

          vmess://...
          vless://...
          trojan://...
          ss://...

        اگر نتواند پروتکل را تشخیص دهد → None.
        """
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
        """
        اگر خروجی bridge دیکت باشد، آن را به یک JSON کاننیکال تبدیل می‌کند.
        اگر رشته باشد، همان را (trim شده) برمی‌گرداند.
        """
        if isinstance(obj, str):
            return obj.strip()
        if isinstance(obj, dict) or isinstance(obj, list):
            # canonical JSON: sort_keys, بدون فاصله اضافه
            return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        # هر چیز دیگری را stringify می‌کنیم تا حداقل قابل ذخیره باشد
        return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)

    def _convert_url_to_outbound(self, url: str) -> Dict[str, Any] | str:
        """
        این تابع نقطهٔ اتصال ما با jsbridge است.

        فرض شده در ماژول app.xraymgr.jsbridge تابعی با امضای زیر وجود دارد:

            def convert_to_outbound(url: str) -> dict | str:
                ...

        که در صورت موفقیت، outbound استاندارد (یا JSON string) برمی‌گرداند
        و در صورت خطا، Exception پرتاب می‌کند.
        """
        return jsbridge.convert_to_outbound(url)

    # ---------- منطق اصلی ----------

    def update_missing_json(self) -> None:
        print(f"[json_updater] starting JSON update job (batch_size={self.batch_size})")

        conn = sqlite3.connect(get_db_path())
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute("PRAGMA temp_store = MEMORY;")

        try:
            last_id = 0

            while True:
                self._check_stopped()

                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT id, url
                    FROM links
                    WHERE (config_json IS NULL OR TRIM(config_json) = '')
                      AND is_invalid = 0
                      AND is_protocol_unsupported = 0
                      AND url IS NOT NULL
                      AND TRIM(url) <> ''
                      AND id > ?
                    ORDER BY id
                    LIMIT ?
                    """,
                    (last_id, self.batch_size),
                )
                rows = cur.fetchall()
                if not rows:
                    print("[json_updater] no more candidates, exiting loop.")
                    break

                batch_size = len(rows)
                with self._stats_lock:
                    self.stats["batches"] += 1
                    self.stats["total_candidates"] += batch_size

                print(
                    f"[json_updater] batch loaded: {batch_size} rows "
                    f"(last_id before batch={last_id})"
                )

                updates_json: List[Tuple[str, int]] = []
                mark_invalid: List[int] = []
                mark_unsupported: List[int] = []

                for row in rows:
                    self._check_stopped()

                    row_id = int(row["id"])
                    last_id = row_id
                    url = row["url"]
                    if url is None:
                        continue

                    url_str = str(url).strip()
                    if not url_str:
                        # URL خالی → invalid
                        mark_invalid.append(row_id)
                        with self._stats_lock:
                            self.stats["urls_failed"] += 1
                        continue

                    with self._stats_lock:
                        self.stats["urls_seen"] += 1

                    proto = self._detect_protocol(url_str)
                    if not proto:
                        # اصلاً پروتکل مشخصی ندارد → invalid
                        print(f"[json_updater] no protocol detected for id={row_id}, url={url_str!r}")
                        mark_invalid.append(row_id)
                        with self._stats_lock:
                            self.stats["urls_failed"] += 1
                        continue

                    # اگر پروتکل ساپورت نمی‌شود → به عنوان unsupported علامت بزن
                    if proto not in self.SUPPORTED_PROTOCOLS:
                        print(
                            f"[json_updater] unsupported protocol {proto!r} "
                            f"for id={row_id}, marking is_protocol_unsupported=1"
                        )
                        mark_unsupported.append(row_id)
                        with self._stats_lock:
                            self.stats["urls_failed"] += 1
                        continue

                    # پروتکل ساپورت‌شده → تلاش برای تبدیل
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

                # اعمال آپدیت‌ها روی DB
                cur = conn.cursor()
                rows_changed = 0

                # ۱) آپدیت config_json
                if updates_json:
                    try:
                        cur.execute("BEGIN")
                        cur.executemany(
                            "UPDATE links SET config_json = ? WHERE id = ?",
                            updates_json,
                        )
                        conn.commit()
                        rows_changed += cur.rowcount
                        print(f"[json_updater] updated config_json for {len(updates_json)} rows "
                              f"(sqlite rowcount={cur.rowcount})")
                    except Exception as e:
                        conn.rollback()
                        print(f"[json_updater] ERROR committing config_json updates: {e}")

                # ۲) علامت‌گذاری invalid
                if mark_invalid:
                    try:
                        cur.execute("BEGIN")
                        cur.executemany(
                            "UPDATE links SET is_invalid = 1 WHERE id = ?",
                            [(rid,) for rid in mark_invalid],
                        )
                        conn.commit()
                        rows_changed += cur.rowcount
                        print(f"[json_updater] marked {len(mark_invalid)} rows as invalid "
                              f"(sqlite rowcount={cur.rowcount})")
                    except Exception as e:
                        conn.rollback()
                        print(f"[json_updater] ERROR marking invalid rows: {e}")

                # ۳) علامت‌گذاری unsupported
                if mark_unsupported:
                    try:
                        cur.execute("BEGIN")
                        cur.executemany(
                            "UPDATE links SET is_protocol_unsupported = 1 WHERE id = ?",
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
        )


if __name__ == "__main__":
    updater = JsonConfigUpdater(batch_size=1000)
    updater.update_missing_json()