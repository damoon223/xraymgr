import secrets
import sqlite3
import string
import threading
from typing import Dict, List

from .schema import init_db_schema
from .settings import get_db_path


class TagUpdaterStopped(Exception):
    """Signal برای توقف graceful tag updater."""
    pass


class OutboundTagUpdater:
    """
    هدف:
      - افزودن ستون‌های inbound_tag / outbound_tag به جدول links (اگر وجود ندارند)
      - ساخت UNIQUE INDEX روی tagها (partial unique index)
      - پر کردن outbound_tag برای تمام ردیف‌هایی که outbound_tag ندارند (NULL یا خالی)

    قرارداد تگ:
      - outbound_tag: قالب x_<6 chars>  (مثال: x_a1B9zK)
      - charset: [A-Za-z0-9]
    """

    DEFAULT_PREFIX = "x_"
    DEFAULT_RAND_LEN = 6
    RAND_ALPHABET = string.ascii_letters + string.digits

    def __init__(
        self,
        batch_size: int = 1000,
        prefix: str = DEFAULT_PREFIX,
        rand_len: int = DEFAULT_RAND_LEN,
        sleep_between_batches: float = 0.0,
    ) -> None:
        self.batch_size = int(batch_size)
        self.prefix = str(prefix)
        self.rand_len = int(rand_len)
        self.sleep_between_batches = float(sleep_between_batches)

        self.stats: Dict[str, int] = {
            "batches": 0,
            "rows_selected": 0,
            "rows_updated": 0,
            "collisions_retried": 0,
        }

        self._stop_event = threading.Event()
        self._stats_lock = threading.Lock()

    # ---------- stop ----------
    def request_stop(self) -> None:
        print("[tag_updater] stop requested")
        self._stop_event.set()

    def _check_stopped(self) -> None:
        if self._stop_event.is_set():
            raise TagUpdaterStopped()

    # ---------- schema ----------
    @staticmethod
    def _table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
        cur = conn.cursor()
        cur.execute(f"PRAGMA table_info({table})")
        return [str(r[1]) for r in cur.fetchall()]

    def ensure_tag_schema(self) -> None:
        """ستون‌ها و ایندکس‌های تگ را اضافه می‌کند (idempotent)."""
        init_db_schema()

        conn = sqlite3.connect(get_db_path())
        try:
            cur = conn.cursor()

            cols = set(self._table_columns(conn, "links"))
            alters: List[str] = []

            if "outbound_tag" not in cols:
                alters.append("ALTER TABLE links ADD COLUMN outbound_tag TEXT")
            if "inbound_tag" not in cols:
                alters.append("ALTER TABLE links ADD COLUMN inbound_tag TEXT")

            for stmt in alters:
                try:
                    cur.execute(stmt)
                except sqlite3.Error as e:
                    print(f"[tag_updater] WARN: schema alter failed: {e} (stmt={stmt})")

            # Partial UNIQUE indexes: فقط برای tagهای غیرخالی
            try:
                cur.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_links_outbound_tag_unique
                    ON links(outbound_tag)
                    WHERE outbound_tag IS NOT NULL AND TRIM(outbound_tag) <> ''
                    """
                )
            except sqlite3.Error as e:
                print(f"[tag_updater] WARN: could not create outbound_tag unique index: {e}")

            try:
                cur.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_links_inbound_tag_unique
                    ON links(inbound_tag)
                    WHERE inbound_tag IS NOT NULL AND TRIM(inbound_tag) <> ''
                    """
                )
            except sqlite3.Error as e:
                print(f"[tag_updater] WARN: could not create inbound_tag unique index: {e}")

            conn.commit()
        finally:
            conn.close()

    # ---------- tag generation ----------
    def generate_tag(self) -> str:
        return self.prefix + "".join(secrets.choice(self.RAND_ALPHABET) for _ in range(self.rand_len))

    # ---------- main ----------
    def run(self) -> None:
        self.fill_missing_outbound_tags()

    def fill_missing_outbound_tags(self) -> None:
        print(f"[tag_updater] starting (batch_size={self.batch_size}, pattern={self.prefix}<rand:{self.rand_len}>)")
        self.ensure_tag_schema()

        conn = sqlite3.connect(get_db_path())
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute("PRAGMA temp_store = MEMORY;")

        try:
            while True:
                self._check_stopped()

                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT id
                    FROM links
                    WHERE outbound_tag IS NULL OR TRIM(outbound_tag) = ''
                    ORDER BY id ASC
                    LIMIT ?
                    """,
                    (self.batch_size,),
                )
                ids = [int(r["id"]) for r in cur.fetchall()]
                if not ids:
                    print("[tag_updater] no more rows with missing outbound_tag.")
                    break

                with self._stats_lock:
                    self.stats["batches"] += 1
                    self.stats["rows_selected"] += len(ids)

                remaining = ids

                # collisionهای نادر UNIQUE را با retry محدود batch-local حل می‌کنیم
                for attempt in range(6):
                    self._check_stopped()
                    if not remaining:
                        break

                    tags = [self.generate_tag() for _ in remaining]
                    params = list(zip(tags, remaining))

                    before = conn.total_changes
                    cur = conn.cursor()
                    cur.executemany(
                        """
                        UPDATE OR IGNORE links
                        SET outbound_tag = ?
                        WHERE id = ? AND (outbound_tag IS NULL OR TRIM(outbound_tag) = '')
                        """,
                        params,
                    )
                    conn.commit()
                    after = conn.total_changes

                    with self._stats_lock:
                        self.stats["rows_updated"] += max(0, after - before)
                        if attempt > 0:
                            self.stats["collisions_retried"] += len(remaining)

                    # باقی‌مانده‌ها را دوباره پیدا می‌کنیم
                    qmarks = ",".join(["?"] * len(remaining))
                    cur = conn.cursor()
                    cur.execute(
                        f"""
                        SELECT id
                        FROM links
                        WHERE id IN ({qmarks})
                          AND (outbound_tag IS NULL OR TRIM(outbound_tag) = '')
                        """,
                        remaining,
                    )
                    remaining = [int(r[0]) for r in cur.fetchall()]

                if remaining:
                    print(
                        f"[tag_updater] WARN: {len(remaining)} ids still missing outbound_tag after retries; "
                        "will retry in next loop."
                    )

                if self.sleep_between_batches > 0:
                    import time

                    time.sleep(self.sleep_between_batches)

        except TagUpdaterStopped:
            print("[tag_updater] stopped by request.")
        finally:
            try:
                conn.close()
            except Exception:
                pass

        print(
            "[tag_updater] finished:"
            f"\n batches: {self.stats['batches']}"
            f"\n rows_selected: {self.stats['rows_selected']}"
            f"\n rows_updated: {self.stats['rows_updated']}"
            f"\n collisions_retried: {self.stats['collisions_retried']}"
        )


if __name__ == "__main__":
    OutboundTagUpdater(batch_size=1000).fill_missing_outbound_tags()
