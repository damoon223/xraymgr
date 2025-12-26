import os
import re
import sqlite3
import threading
from typing import List, Optional

from .settings import BASE_DIR, get_db_path


class ImporterStopped(Exception):
    """Signal برای توقف graceful ایمپورتر."""


class RawConfigImporter:
    """
    ایمپورت کانفیگ‌ها از فایل raw به جدول links، و سپس نرمالایز URLها.

    دو مرحله اصلی:
      1) خواندن data/raw/raw_configs.txt و وارد کردن هر خط به ستون url در links
         (با INSERT OR IGNORE برای جلوگیری از تکرار).
      2) نرمالایز URLها در جدول links (multi-link splitter).

    مرحلهٔ اضافه (طبق درخواست جدید):
      3) بعد از نرمالایز کردن، هر پروتکلی غیر از vless/vmess/ss/trojan
         تیک is_protocol_unsupported می‌خورد و دیگر انتخاب نمی‌شود.
    """

    # برای split کردن multi-config URLها (عمداً گسترده‌تر از ساپورت نهایی است)
    _PROTO_RE = re.compile(
        r"(vmess|vless|trojan|ssr|ss|shadowsocks2022|shadowsocks|hysteria2|hysteria|hy2|tuic)://",
        re.IGNORECASE,
    )

    # فقط این‌ها ساپورت هستند؛ غیر از این‌ها => is_protocol_unsupported=1
    _SUPPORTED_PROTOCOLS = {"vmess", "vless", "ss", "trojan"}

    def __init__(self, batch_size: int = 1000) -> None:
        self.batch_size = int(batch_size)
        self.stats = {
            "total_lines": 0,
            "valid_configs": 0,
            "inserted": 0,
            "batches_committed": 0,
            "normalize_batches": 0,
            "normalize_candidates": 0,
            "normalized_rows": 0,
            "normalized_new_links": 0,
            "unsupported_batches": 0,
            "unsupported_scanned": 0,
            "unsupported_marked": 0,
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

    # ---------- schema helpers ----------

    @staticmethod
    def _table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
        cur = conn.cursor()
        cur.execute(f"PRAGMA table_info({table})")
        return [str(r[1]) for r in cur.fetchall()]

    @classmethod
    def _ensure_links_columns(cls, conn: sqlite3.Connection) -> None:
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
                print(f"[importer] WARN: schema alter failed: {e} (stmt={stmt})")
        conn.commit()

    # ---------- مسیر فایل raw ----------

    @staticmethod
    def _get_raw_file_path() -> str:
        return os.path.join(os.fspath(BASE_DIR), "data", "raw", "raw_configs.txt")

    # ---------- ابزارهای نرمال‌سازی URL ----------

    @staticmethod
    def _detect_protocol(url: str) -> Optional[str]:
        if not url:
            return None
        s = str(url).strip()
        m = re.match(r"^([a-zA-Z0-9+.\-]+)://", s)
        if not m:
            return None
        p = (m.group(1) or "").strip().lower()
        return p or None

    def _split_multi_config_url(self, url: str) -> List[str]:
        if not url:
            return []
        s = str(url).strip()
        if not s:
            return []

        matches = list(self._PROTO_RE.finditer(s))
        if not matches:
            return [s]
        if len(matches) == 1:
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
            print(f"[importer] raw configs file not found at {raw_path!r}, skipping import step.")
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
                        continue

                    with self._stats_lock:
                        self.stats["valid_configs"] += 1

                    try:
                        cur.execute("INSERT OR IGNORE INTO links (url) VALUES (?)", (stripped,))
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
                            print(f"[importer] committed batch of {batch_ops} inserts (last line_no={line_no})")
                        except sqlite3.Error as e:
                            print(f"[importer] ERROR committing insert batch: {e}")
                        batch_ops = 0

        except FileNotFoundError:
            print(f"[importer] raw file disappeared during import: {raw_path!r}")
        except UnicodeDecodeError as e:
            print(f"[importer] encoding error while reading raw file: {e}")

        if batch_ops > 0:
            try:
                conn.commit()
                with self._stats_lock:
                    self.stats["batches_committed"] += 1
                print(f"[importer] committed final batch of {batch_ops} inserts")
            except sqlite3.Error as e:
                print(f"[importer] ERROR committing final insert batch: {e}")

    # ---------- مرحلهٔ ۲: نرمالایز URLها در DB ----------

    def _normalize_links(self, conn: sqlite3.Connection) -> None:
        print("[importer] starting URL normalization pass (multi-link splitter)")

        cols = set(self._table_columns(conn, "links"))
        has_is_invalid = "is_invalid" in cols
        has_unsupported = "is_protocol_unsupported" in cols

        last_id = 0

        while True:
            self._check_stopped()

            where_parts = [
                "(config_hash IS NULL OR config_hash = '')",
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
                if len(parts) <= 1:
                    continue

                with self._stats_lock:
                    self.stats["normalize_candidates"] += 1

                new_inserted = 0

                for part in parts:
                    try:
                        cur.execute("INSERT OR IGNORE INTO links (url) VALUES (?)", (part,))
                        if cur.rowcount == 1:
                            new_inserted += 1
                    except sqlite3.Error as e:
                        print(f"[importer] ERROR inserting normalized url part for id={row_id}: {e}")

                try:
                    if has_is_invalid:
                        cur.execute("UPDATE links SET is_invalid = 1 WHERE id = ?", (row_id,))
                    else:
                        cur.execute(
                            "UPDATE links SET needs_replace = 1, is_alive = 0 WHERE id = ?",
                            (row_id,),
                        )
                except sqlite3.Error as e:
                    print(f"[importer] ERROR marking multi-link row id={row_id} as invalid: {e}")
                else:
                    changes += 1
                    with self._stats_lock:
                        self.stats["normalized_rows"] += 1
                        self.stats["normalized_new_links"] += new_inserted

            if changes > 0:
                try:
                    conn.commit()
                except sqlite3.Error as e:
                    print(f"[importer] ERROR committing normalization batch: {e}")

        print("[importer] URL normalization pass finished.")

    # ---------- مرحلهٔ ۳: علامت‌گذاری پروتکل‌های غیرپشتیبانی‌شده ----------

    def _mark_unsupported_protocols(self, conn: sqlite3.Connection) -> None:
        cols = set(self._table_columns(conn, "links"))
        has_is_invalid = "is_invalid" in cols
        has_unsupported = "is_protocol_unsupported" in cols

        if not has_unsupported:
            print("[importer] is_protocol_unsupported column not found; skipping unsupported marking.")
            return

        print("[importer] marking unsupported protocols (after normalization)")

        last_id = 0

        while True:
            self._check_stopped()

            where_parts = [
                "url IS NOT NULL",
                "TRIM(url) <> ''",
                "is_protocol_unsupported = 0",
                "id > ?",
            ]
            if has_is_invalid:
                where_parts.append("is_invalid = 0")

            sql = (
                "SELECT id, url FROM links "
                f"WHERE {' AND '.join(where_parts)} "
                "ORDER BY id LIMIT ?"
            )

            cur = conn.cursor()
            cur.execute(sql, (last_id, self.batch_size))
            rows = cur.fetchall()
            if not rows:
                break

            with self._stats_lock:
                self.stats["unsupported_batches"] += 1

            to_mark: List[int] = []

            for row in rows:
                self._check_stopped()

                row_id = int(row["id"])
                last_id = row_id

                url = str(row["url"] or "").strip()
                if not url:
                    continue

                with self._stats_lock:
                    self.stats["unsupported_scanned"] += 1

                proto = self._detect_protocol(url)
                if proto and proto not in self._SUPPORTED_PROTOCOLS:
                    to_mark.append(row_id)

            if not to_mark:
                continue

            try:
                cur.execute("BEGIN")
                if has_is_invalid:
                    cur.executemany(
                        "UPDATE links SET is_protocol_unsupported = 1, is_invalid = 0 WHERE id = ?",
                        [(i,) for i in to_mark],
                    )
                else:
                    cur.executemany(
                        "UPDATE links SET is_protocol_unsupported = 1 WHERE id = ?",
                        [(i,) for i in to_mark],
                    )
                conn.commit()
                with self._stats_lock:
                    self.stats["unsupported_marked"] += len(to_mark)
                print(f"[importer] marked {len(to_mark)} rows as protocol_unsupported")
            except sqlite3.Error as e:
                conn.rollback()
                print(f"[importer] ERROR marking protocol_unsupported: {e}")

        print("[importer] unsupported protocol marking finished.")

    # ---------- نقطهٔ ورود اصلی ----------

    def import_configs(self) -> None:
        print(f"[importer] starting import job (batch_size={self.batch_size})")

        conn = sqlite3.connect(get_db_path())
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute("PRAGMA temp_store = MEMORY;")

        try:
            try:
                self._ensure_links_columns(conn)
            except Exception as e:
                print(f"[importer] WARN: could not ensure schema columns: {e}")

            self._import_from_raw_file(conn)
            self._normalize_links(conn)
            self._mark_unsupported_protocols(conn)

        except ImporterStopped:
            print("[importer] stopped by request.")
        finally:
            try:
                conn.close()
                print("[importer] DB connection closed.")
            except Exception:
                pass

        print(
            "[importer] job finished:"
            f"\n total_lines (raw file): {self.stats['total_lines']}"
            f"\n valid_configs (non-empty lines): {self.stats['valid_configs']}"
            f"\n inserted (new links): {self.stats['inserted']}"
            f"\n batches_committed (import): {self.stats['batches_committed']}"
            f"\n normalize_batches: {self.stats['normalize_batches']}"
            f"\n normalize_candidates (multi-link urls): {self.stats['normalize_candidates']}"
            f"\n normalized_rows (original multi-link rows marked invalid): {self.stats['normalized_rows']}"
            f"\n normalized_new_links (links inserted from splits): {self.stats['normalized_new_links']}"
            f"\n unsupported_batches: {self.stats['unsupported_batches']}"
            f"\n unsupported_scanned: {self.stats['unsupported_scanned']}"
            f"\n unsupported_marked: {self.stats['unsupported_marked']}"
        )


if __name__ == "__main__":
    importer = RawConfigImporter(batch_size=1000)
    importer.import_configs()
