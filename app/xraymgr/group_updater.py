import sqlite3
from typing import Optional, Dict

from .settings import get_db_path


class ConfigGroupUpdater:
    """
    Job مخصوص گروهبندی کانفیگ‌ها بر اساس config_hash.

    منطق:
      - فقط ردیف‌هایی که config_hash پر و config_group_id خالی دارند پردازش می‌شوند.
      - برای هر config_hash:
          * اگر قبلاً گروهی برای آن ساخته شده باشد (config_group_id غیر خالی):
                همان group_id برای ردیف‌های جدید استفاده می‌شود.
          * اگر گروهی وجود نداشته باشد:
                کمترین id برای آن hash به‌عنوان پرایمری انتخاب می‌شود و
                config_group_id همه‌ی ردیف‌های آن hash برابر همان id (به صورت TEXT) می‌شود.
    """

    def __init__(self, batch_size: int = 500):
        self.batch_size = batch_size
        self.stats: Dict[str, int] = {
            "total_candidate_hashes": 0,  # تعداد hashهایی که باید پردازش شوند
            "hashes_seen": 0,             # چند hash واقعاً پردازش شد
            "batches": 0,                 # چند batch اجرا شد
            "groups_created": 0,          # چند گروه جدید ساخته شد
            "rows_grouped": 0,            # چند ردیف config_group_id گرفتند
        }
        self._stop_requested = False

    def request_stop(self) -> None:
        """برای توقف ملایم job از بیرون (مثلاً از پنل وب)."""
        self._stop_requested = True

    def _get_connection(self) -> sqlite3.Connection:
        db_path = get_db_path()
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        return conn

    # ------------ کوئری‌های کمکی ------------

    def _count_total_candidate_hashes(self, cur: sqlite3.Cursor) -> int:
        """
        شمارش تقریبی تعداد hashهایی که هنوز config_group_id خالی دارند.
        فقط برای آمار؛ اگر دیتابیس خیلی بزرگ است می‌شود این را حذف کرد.
        """
        cur.execute(
            """
            SELECT COUNT(DISTINCT config_hash) AS cnt
            FROM links
            WHERE config_hash IS NOT NULL
              AND config_hash != ''
              AND (config_group_id IS NULL OR config_group_id = '')
            """
        )
        row = cur.fetchone()
        return int(row["cnt"]) if row and row["cnt"] is not None else 0

    def _fetch_next_hash_batch(self, cur: sqlite3.Cursor) -> list[str]:
        """
        گرفتن batch بعدی از config_hash‌هایی که هنوز group نشده‌اند.
        """
        cur.execute(
            """
            SELECT DISTINCT config_hash
            FROM links
            WHERE config_hash IS NOT NULL
              AND config_hash != ''
              AND (config_group_id IS NULL OR config_group_id = '')
            LIMIT ?
            """,
            (self.batch_size,),
        )
        rows = cur.fetchall()
        return [row["config_hash"] for row in rows]

    def _find_existing_group_id(self, cur: sqlite3.Cursor, h: str) -> Optional[str]:
        """
        اگر قبلاً برای این hash گروهی ساخته شده باشد، یک config_group_id برمی‌گرداند.
        """
        cur.execute(
            """
            SELECT config_group_id
            FROM links
            WHERE config_hash = ?
              AND config_group_id IS NOT NULL
              AND config_group_id != ''
            LIMIT 1
            """,
            (h,),
        )
        row = cur.fetchone()
        if row:
            gid = row["config_group_id"]
            if gid:
                return str(gid)
        return None

    def _select_primary_id_for_hash(self, cur: sqlite3.Cursor, h: str) -> Optional[int]:
        """
        انتخاب id پرایمری برای یک hash (کمترین id).
        """
        cur.execute(
            """
            SELECT id
            FROM links
            WHERE config_hash = ?
            ORDER BY id
            LIMIT 1
            """,
            (h,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return int(row["id"])

    def _ensure_primary_flag(self, cur: sqlite3.Cursor, primary_id: Optional[int]) -> None:
        """
        مطمئن می‌شود ردیف پرایمری is_config_primary = 1 داشته باشد.
        """
        if primary_id is None:
            return
        cur.execute(
            """
            UPDATE links
            SET is_config_primary = 1
            WHERE id = ?
            """,
            (primary_id,),
        )

    # ------------ پردازش هر hash ------------

    def _process_single_hash(self, cur: sqlite3.Cursor, h: str) -> None:
        # سعی می‌کنیم اگر قبلاً گروپی برای این hash وجود دارد، از همان استفاده کنیم
        group_id = self._find_existing_group_id(cur, h)
        primary_id: Optional[int] = None

        if group_id is None:
            # هیچ گروپی وجود ندارد → ساخت گروه جدید
            primary_id = self._select_primary_id_for_hash(cur, h)
            if primary_id is None:
                return
            group_id = str(primary_id)
            self.stats["groups_created"] += 1
            # این ردیف را به‌طور صریح به‌عنوان پرایمری علامت می‌زنیم
            self._ensure_primary_flag(cur, primary_id)
        else:
            # اگر group_id شبیه عدد بود، پرایمری را از روی آن حدس می‌زنیم
            try:
                primary_id = int(group_id)
            except (TypeError, ValueError):
                primary_id = None

        # اختصاص group_id به همه‌ی ردیف‌هایی که این hash را دارند و هنوز group نشده‌اند
        cur.execute(
            """
            UPDATE links
            SET config_group_id = ?,
                is_config_primary = CASE WHEN id = ? THEN 1 ELSE is_config_primary END
            WHERE config_hash = ?
              AND (config_group_id IS NULL OR config_group_id = '')
            """,
            (group_id, primary_id if primary_id is not None else -1, h),
        )
        rows_affected = cur.rowcount or 0
        self.stats["rows_grouped"] += rows_affected

    # ------------ حلقهٔ اصلی job ------------

    def update_groups(self) -> None:
        """
        اجرای job گروه‌بندی کانفیگ‌ها بر اساس config_hash.

        تکرارش امن است:
          - فقط روی ردیف‌هایی کار می‌کند که config_group_id خالی دارند.
          - اجرای چندباره فقط ردیف‌های جدید (یا عقب‌افتاده) را گروه‌بندی می‌کند.
        """
        conn = self._get_connection()
        try:
            cur = conn.cursor()

            # تعداد hashهای کاندید (فقط برای آمار)
            total_candidates = self._count_total_candidate_hashes(cur)
            self.stats["total_candidate_hashes"] = total_candidates
            print(f"[group_updater] total candidate hashes: {total_candidates}")

            batch_index = 0
            while not self._stop_requested:
                hashes = self._fetch_next_hash_batch(cur)
                if not hashes:
                    print("[group_updater] no more hashes to process.")
                    break

                batch_index += 1
                self.stats["batches"] += 1
                print(f"[group_updater] batch {batch_index}: processing {len(hashes)} hashes...")

                for h in hashes:
                    if self._stop_requested:
                        print("[group_updater] stop requested, breaking loop.")
                        break
                    if h is None or h == "":
                        continue
                    self.stats["hashes_seen"] += 1
                    self._process_single_hash(cur, h)

                # هر batch یک commit جدا داشته باشد
                conn.commit()

            print(
                "[group_updater] done. "
                f"batches={self.stats['batches']}, "
                f"hashes_seen={self.stats['hashes_seen']}, "
                f"groups_created={self.stats['groups_created']}, "
                f"rows_grouped={self.stats['rows_grouped']}"
            )

        finally:
            conn.close()


def main() -> None:
    """
    اجرای job از خط فرمان:
      python -m xraymgr.group_updater
    (بسته به این‌که محیط پروژه‌ات چطور راه‌اندازی شده است)
    """
    updater = ConfigGroupUpdater(batch_size=500)
    updater.update_groups()
    print("[group_updater] stats:", updater.stats)


if __name__ == "__main__":
    main()
