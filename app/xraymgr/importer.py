import os
import re
import sqlite3
import threading
from typing import List

from .settings import get_db_path


class ImporterStopped(Exception):
    """Signal برای توقف graceful ایمپورتر."""
    pass


class RawConfigImporter:
    """
    ایمپورت کانفیگ‌ها از فایل raw به جدول links، و سپس نرمالایز URLها.

    دو مرحله اصلی:

    1) خواندن data/raw/raw_configs.txt و وارد کردن هر خط به ستون url در links
       (با INSERT OR IGNORE برای جلوگیری از تکرار).

    2) نرمالایز URLها در جدول links:
       - فقط ردیف‌هایی که:
         * config_hash خالی دارند
         * is_invalid = 0
         * is_protocol_unsupported = 0
         * url غیرخالی دارند
       - اگر در یک url چندین کانفیگ (vmess/vless/ss/trojan/hysteria2/hy2/tuic/…) چسبیده باشد،
         آن را به چند url جدا می‌شکنیم، برای هرکدام رکورد جدید می‌سازیم (INSERT OR IGNORE)،
         و رکورد اصلی را به‌جای حذف، به‌صورت is_invalid = 1 علامت‌گذاری می‌کنیم.

    stats (برای پنل وب):
      - total_lines: تعداد کل خطوط خوانده‌شده از فایل raw
      - valid_configs: تعداد خطوط غیرخالی که به‌عنوان کاندید ایمپورت حساب شده‌اند
      - inserted: تعداد رکوردهای جدیدی که واقعاً در links درج شده‌اند
      - batches_committed: تعداد دفعات commit در مرحلهٔ ایمپورت

    آمار اضافه:
      - normalize_batches: تعداد batchهای نرمالایز
      - normalize_candidates: تعداد URLهایی که multi-link تشخیص داده شده‌اند
      - normalized_rows: تعداد رکوردهای اصلیِ multi-link که بعد از split به‌عنوان invalid علامت‌گذاری شده‌اند
      - normalized_new_links: تعداد لینک‌های جدیدی که در نتیجهٔ نرمالایز درج شدند
    """

    # برای تشخیص مرز پروتکل‌ها در URL.
    # علاوه بر پروتکل‌های ساپورت‌شده در JSON updater، پروتکل‌های رایج دیگری مثل
    # hysteria2 / hy2 / tuic / ssr را هم لحاظ می‌کنیم تا multi-linkهای ترکیبی
    # (مثل ss + hysteria2 + ...) هم شکسته شوند.
    _PROTO_RE = re.compile(
        r"(vmess|vless|trojan|ssr|ss|shadowsocks2022|shadowsocks|hysteria2|hysteria|hy2|tuic)://",
        re.IGNORECASE,
    )

    def __init__(self, batch_size: int = 1000) -> None:
        self.batch_size = batch_size
        self.stats = {
            "total_lines": 0,
            "valid_configs": 0,
            "inserted": 0,
            "batches_committed": 0,
            # stats نرمالایزر:
            "normalize_batches": 0,
            "normalize_candidates": 0,
            "normalized_rows": 0,
            "normalized_new_links": 0,
        }
        self._stop_event = threading.Event()
        self._stats_lock = threading.Lock()

    # ---------- کنترل توقف ----------

    def request_stop(self) -> None:
        print("[importer] stop requested")
        self._stop_event.set()

    def _check_stopped(self) -> None:
        if self._stop_event.is_set():
            raise ImporterStopped()

    # ---------- مسیر فایل raw ----------

    @staticmethod
    def _get_raw_file_path() -> str:
        """
        مسیر فایل raw_configs.txt بر اساس ساختار repo:

        /opt/xraymgr/app/xraymgr/importer.py  →  __file__
        /opt/xraymgr                         →  BASE_DIR
        /opt/xraymgr/data/raw/raw_configs.txt → فایل ورودی
        """
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        return os.path.join(base_dir, "data", "raw", "raw_configs.txt")

    # ---------- ابزارهای نرمال‌سازی URL ----------

    def _split_multi_config_url(self, url: str) -> List[str]:
        """
        یک رشتهٔ url که ممکن است چند کانفیگ چسبیده داخلش باشد را
        به چند url جداگانه تبدیل می‌کند.

        مثال:
            vless://.....vmess://.....ss://...
            →
            [
              'vless://.....',
              'vmess://.....',
              'ss://...'
            ]

        اگر فقط یک پروتکل پیدا شود → [url] برمی‌گرداند.
        اگر هیچ پروتکلی پیدا نشود → [url] (کاری به آن نداریم).
        """
        if not url:
            return []

        s = str(url).strip()
        if not s:
            return []

        matches = list(self._PROTO_RE.finditer(s))
        if not matches:
            # نمی‌دانیم چیست؛ به‌عنوان یک URL تکی در نظر گرفته می‌شود
            return [s]

        if len(matches) == 1:
            # تنها یک پروتکل → همین URL تکی است
            return [s]

        parts: List[str] = []
        for idx, m in enumerate(matches):
            start = m.start()
            end = matches[idx + 1].start() if idx + 1 < len(matches) else len(s)
            segment = s[start:end].strip()
            if segment:
                parts.append(segment)

        return parts

    # ---------- مرحلهٔ ۱: ایمپورت از فایل raw ----------

    def _import_from_raw_file(self, conn: sqlite3.Connection) -> None:
        raw_path = self._get_raw_file_path()
        if not os.path.exists(raw_path):
            print(
                f"[importer] raw configs file not found at {raw_path!r}, "
                f"skipping import step."
            )
            return

        print(f"[importer] importing from raw file: {raw_path!r}")
        cur = conn.cursor()
        batch_ops = 0

        try:
            with open(raw_path, "r", encoding="utf-8") as f:
                for line_no, line in enumerate(f, start=1):
                    self._check_stopped()

                    stripped = line.strip()
                    with self._stats_lock:
                        self.stats["total_lines"] += 1

                    if not stripped:
                        # خط خالی
                        continue

                    with self._stats_lock:
                        self.stats["valid_configs"] += 1

                    try:
                        cur.execute(
                            "INSERT OR IGNORE INTO links (url) VALUES (?)",
                            (stripped,),
                        )
                        if cur.rowcount == 1:
                            with self._stats_lock:
                                self.stats["inserted"] += 1
                    except sqlite3.Error as e:
                        print(f"[importer] ERROR inserting line {line_no}: {e}")
                        continue

                    batch_ops += 1
                    if batch_ops >= self.batch_size:
                        try:
                            conn.commit()
                            with self._stats_lock:
                                self.stats["batches_committed"] += 1
                            print(
                                f"[importer] committed batch of {batch_ops} inserts "
                                f"(last line_no={line_no})"
                            )
                        except sqlite3.Error as e:
                            print(
                                f"[importer] ERROR committing insert batch: {e}"
                            )
                        batch_ops = 0

        except FileNotFoundError:
            print(
                f"[importer] raw file disappeared during import: {raw_path!r}"
            )
        except UnicodeDecodeError as e:
            print(
                f"[importer] encoding error while reading raw file: {e}"
            )

        # commit نهایی اگر چیزی مانده باشد
        if batch_ops > 0:
            try:
                conn.commit()
                with self._stats_lock:
                    self.stats["batches_committed"] += 1
                print(
                    f"[importer] committed final batch of {batch_ops} inserts"
                )
            except sqlite3.Error as e:
                print(
                    f"[importer] ERROR committing final insert batch: {e}"
                )

    # ---------- مرحلهٔ ۲: نرمالایز URLها در DB ----------

    def _normalize_links(self, conn: sqlite3.Connection) -> None:
        """
        روی جدول links پاس نرمالایز اجرا می‌کند:

        - فقط ردیف‌هایی که:
          * config_hash خالی دارند
          * is_invalid = 0
          * is_protocol_unsupported = 0
          * url غیرخالی دارند

        - اگر url شامل چندین پروتکل vmess/vless/ss/trojan/hysteria2/hy2/tuic/ssr/...
          باشد، به چند url جداگانه شکسته می‌شود، برای هرکدام INSERT OR IGNORE می‌زنیم،
          و رکورد اصلی multi-link را حذف نمی‌کنیم، بلکه آن را با is_invalid = 1 علامت‌گذاری می‌کنیم.
        """
        print("[importer] starting URL normalization pass (multi-link splitter)")

        last_id = 0
        while True:
            self._check_stopped()

            cur = conn.cursor()
            cur.execute(
                """
                SELECT id, url
                FROM links
                WHERE (config_hash IS NULL OR config_hash = '')
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
                break

            with self._stats_lock:
                self.stats["normalize_batches"] += 1

            changes = 0

            for row in rows:
                self._check_stopped()

                row_id = int(row["id"])
                last_id = row_id
                url = str(row["url"])

                parts = self._split_multi_config_url(url)

                # اگر فقط یک بخش باشد، نیازی به نرمالایز نیست
                if len(parts) <= 1:
                    continue

                with self._stats_lock:
                    self.stats["normalize_candidates"] += 1

                new_inserted = 0
                for part in parts:
                    try:
                        cur.execute(
                            "INSERT OR IGNORE INTO links (url) VALUES (?)",
                            (part,),
                        )
                        if cur.rowcount == 1:
                            new_inserted += 1
                    except sqlite3.Error as e:
                        print(
                            "[importer] ERROR inserting normalized url part "
                            f"for id={row_id}: {e}"
                        )

                # رکورد اصلی multi-link را حذف نمی‌کنیم؛ به‌جای آن invalid می‌کنیم
                try:
                    cur.execute(
                        "UPDATE links SET is_invalid = 1 WHERE id = ?",
                        (row_id,),
                    )
                except sqlite3.Error as e:
                    print(
                        f"[importer] ERROR marking multi-link row id={row_id} "
                        f"as invalid: {e}"
                    )
                else:
                    changes += 1
                    with self._stats_lock:
                        self.stats["normalized_rows"] += 1
                        self.stats["normalized_new_links"] += new_inserted

            if changes > 0:
                try:
                    conn.commit()
                except sqlite3.Error as e:
                    print(
                        f"[importer] ERROR committing normalization batch: {e}"
                    )

        print("[importer] URL normalization pass finished.")

    # ---------- نقطهٔ ورود اصلی ----------

    def import_configs(self) -> None:
        """
        نقطهٔ اصلی اجرای job ایمپورتر:

        1) ایمپورت از فایل raw_configs.txt به جدول links
        2) نرمالایز URLها در جدول links (multi-link splitter)
        """
        print(f"[importer] starting import job (batch_size={self.batch_size})")

        conn = sqlite3.connect(get_db_path())
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute("PRAGMA temp_store = MEMORY;")

        try:
            # مرحله ۱: ایمپورت از فایل
            self._import_from_raw_file(conn)

            # مرحله ۲: نرمالایز URLها در DB
            self._normalize_links(conn)

        except ImporterStopped:
            print("[importer] stopped by request.")
        finally:
            try:
                conn.close()
                print("[importer] DB connection closed.")
            except Exception:
                pass

        # گزارش نهایی
        print(
            "[importer] job finished:"
            f"\n  total_lines (raw file): {self.stats['total_lines']}"
            f"\n  valid_configs (non-empty lines): {self.stats['valid_configs']}"
            f"\n  inserted (new links): {self.stats['inserted']}"
            f"\n  batches_committed (import): {self.stats['batches_committed']}"
            f"\n  normalize_batches: {self.stats['normalize_batches']}"
            f"\n  normalize_candidates (multi-link urls): "
            f"{self.stats['normalize_candidates']}"
            f"\n  normalized_rows (original multi-link rows marked invalid): "
            f"{self.stats['normalized_rows']}"
            f"\n  normalized_new_links (links inserted from splits): "
            f"{self.stats['normalized_new_links']}"
        )


if __name__ == "__main__":
    importer = RawConfigImporter(batch_size=1000)
    importer.import_configs()
