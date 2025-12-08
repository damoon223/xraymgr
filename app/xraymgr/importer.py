import os
import threading
import sqlite3
from typing import Optional, List

from .settings import get_db_path

PROTO_PREFIXES = (
    "vmess://",
    "vless://",
    "trojan://",
    "ss://",
    "ssr://",
    "tuic://",
    "hysteria2://",
    "hy2://",
)


class ImporterStopped(Exception):
    """Signal برای توقف graceful ایمپورتر."""
    pass


class RawConfigImporter:
    """
    ایمپورتر برای خواندن raw_configs.txt و پر کردن جدول links.

    منطق فعلی:
      - هر خط یک url کانفیگ است.
      - فقط خطوطی که با یکی از پروتکل‌های موردنظر شروع شوند قبول می‌شوند.
      - در batchهای ۱۰۰۰تایی INSERT OR IGNORE می‌شوند.
      - فقط ستون url را ست می‌کنیم.
        بقیه ستون‌ها:
          * اگر DEFAULT دارند (test_stage, is_alive, is_in_use, needs_replace)
              → مقدار default تنظیم می‌شود.
          * اگر DEFAULT ندارند → NULL می‌شوند (config_json, config_hash, is_config_primary, ip, ...)
      - config_hash بعداً وقتی JSON آماده شد محاسبه و ست می‌شود.
    """

    def __init__(
        self,
        raw_file: Optional[str] = None,
        batch_size: int = 1000,
    ) -> None:
        if raw_file is None:
            # طبق ساختار پروژه:
            # /opt/xraymgr/data/raw/raw_configs.txt
            raw_file = "/opt/xraymgr/data/raw/raw_configs.txt"

        self.raw_file = raw_file
        self.batch_size = batch_size
        self._stop_event = threading.Event()

        self.stats = {
            "total_lines": 0,
            "valid_configs": 0,
            "inserted": 0,
            "skipped_invalid": 0,
            "batches_committed": 0,
        }

    # ---------- کنترل توقف ----------

    def request_stop(self) -> None:
        print("[importer] stop requested")
        self._stop_event.set()

    def _check_stopped(self):
        if self._stop_event.is_set():
            raise ImporterStopped()

    # ---------- منطق اصلی ----------

    def _iter_valid_configs(self):
        if not os.path.exists(self.raw_file):
            print(f"[importer] raw configs file not found: {self.raw_file}")
            return

        print(f"[importer] reading raw configs from: {self.raw_file}")

        with open(self.raw_file, "r", encoding="utf-8") as f:
            for line_number, line in enumerate(f, start=1):
                self._check_stopped()
                self.stats["total_lines"] += 1

                url = line.strip()
                if not url:
                    self.stats["skipped_invalid"] += 1
                    continue

                if not url.startswith(PROTO_PREFIXES):
                    self.stats["skipped_invalid"] += 1
                    continue

                self.stats["valid_configs"] += 1
                yield url

    def _get_connection(self) -> sqlite3.Connection:
        db_path = get_db_path()
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def import_configs(self):
        """
        نقطه‌ی اصلی اجرا؛ در یک thread background از طریق وب صدا زده می‌شود.
        """
        print("[importer] starting import job...")
        print(f"[importer] using raw file: {self.raw_file}")

        conn = self._get_connection()
        try:
            cur = conn.cursor()

            batch: List[tuple] = []

            for url in self._iter_valid_configs() or []:
                self._check_stopped()

                # فقط url را در batch می‌گذاریم
                batch.append((url,))

                if len(batch) >= self.batch_size:
                    self._insert_batch(cur, batch)
                    conn.commit()
                    self.stats["batches_committed"] += 1
                    print(
                        f"[importer] committed batch of {len(batch)} items "
                        f"(total inserted: {self.stats['inserted']})"
                    )
                    batch.clear()

            # batch باقی‌مانده
            if batch:
                self._insert_batch(cur, batch)
                conn.commit()
                self.stats["batches_committed"] += 1
                print(
                    f"[importer] committed final batch of {len(batch)} items "
                    f"(total inserted: {self.stats['inserted']})"
                )

            print(
                "\n[importer] import finished:"
                f"\n   total lines:      {self.stats['total_lines']}"
                f"\n   valid configs:    {self.stats['valid_configs']}"
                f"\n   inserted (new):   {self.stats['inserted']}"
                f"\n   skipped invalid:  {self.stats['skipped_invalid']}"
                f"\n   batches committed:{self.stats['batches_committed']}"
            )

        except ImporterStopped:
            print(
                "\n[importer] import stopped by request:"
                f"\n   total lines:      {self.stats['total_lines']}"
                f"\n   valid configs:    {self.stats['valid_configs']}"
                f"\n   inserted (new):   {self.stats['inserted']}"
                f"\n   skipped invalid:  {self.stats['skipped_invalid']}"
                f"\n   batches committed:{self.stats['batches_committed']}"
            )
        finally:
            conn.close()
            print("[importer] connection closed.")

    def _insert_batch(self, cur, batch: List[tuple]):
        """
        اجرای batch insert با INSERT OR IGNORE تا urlهای تکراری خطا ندهند.
        فقط ستون url را ست می‌کنیم.
        """
        cur.executemany(
            """
            INSERT OR IGNORE INTO links (
                url
            )
            VALUES (?)
            """,
            batch,
        )
        if cur.rowcount is not None and cur.rowcount > 0:
            self.stats["inserted"] += cur.rowcount


if __name__ == "__main__":
    print("XrayMgr Raw Config Importer")
    importer = RawConfigImporter(batch_size=1000)
    try:
        importer.import_configs()
    except ImporterStopped:
        print("Importer stopped by user.")
