import sqlite3
from typing import Optional, Dict, List

from .settings import get_db_path


class ConfigGroupUpdater:
    """
    Job گروه‌بندی کانفیگ‌ها بر اساس config_hash.

    رفتار مورد انتظار:
      1) فقط برای «جدیدها» group_id تعیین کند:
         یعنی hashهایی که هنوز حداقل یک ردیف با config_group_id NULL/'' دارند.
         - اگر group_id از قبل برای همان hash وجود دارد، فقط NULL/'' ها را با همان group_id پر می‌کند.
         - اگر group_id وجود ندارد، group_id را برابر min(id) همان hash می‌گذارد.
         - group_id های از قبل ست‌شده را دست نمی‌زند (ریچک/ری‌رایت نمی‌کند).

      2) برای همهٔ «گروه‌های کامل» (یعنی hashهایی که دیگر هیچ NULL/'' ندارند)،
         فقط enforce کند که «اولی» (min(id)) primary باشد.
         - اگر already درست است، هیچ کاری نمی‌کند.
    """

    def __init__(self, batch_size: int = 500) -> None:
        self.batch_size = batch_size
        self.stats: Dict[str, int] = {
            "batches_group": 0,
            "hashes_grouped": 0,
            "rows_grouped": 0,
            "groups_created": 0,
            "batches_primary": 0,
            "hashes_primary_fixed": 0,
            "rows_primary_fixed": 0,
        }
        self._stop_requested = False

    def request_stop(self) -> None:
        self._stop_requested = True

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(get_db_path())
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    # -------------------------------
    # Batch selectors
    # -------------------------------

    def _fetch_hashes_needing_group(self, cur: sqlite3.Cursor) -> List[str]:
        """
        فقط hashهایی که هنوز حداقل یک ردیف با config_group_id NULL/'' دارند.
        """
        cur.execute(
            """
            SELECT DISTINCT l.config_hash
            FROM links l
            WHERE l.config_hash IS NOT NULL
              AND l.config_hash != ''
              AND EXISTS (
                SELECT 1
                FROM links u
                WHERE u.config_hash = l.config_hash
                  AND (u.config_group_id IS NULL OR u.config_group_id = '')
              )
            LIMIT ?
            """,
            (self.batch_size,),
        )
        rows = cur.fetchall()
        return [str(r["config_hash"]) for r in rows if r and r["config_hash"]]

    def _fetch_hashes_needing_primary_fix(self, cur: sqlite3.Cursor) -> List[str]:
        """
        فقط hashهایی که «گروه کامل» هستند (هیچ NULL/'' ندارند) و primary آن‌ها اشتباه است:
          - primary_count != 1
          یا
          - primary_id != min_id
        """
        cur.execute(
            """
            SELECT DISTINCT l.config_hash
            FROM links l
            WHERE l.config_hash IS NOT NULL
              AND l.config_hash != ''
              AND NOT EXISTS (
                SELECT 1
                FROM links u
                WHERE u.config_hash = l.config_hash
                  AND (u.config_group_id IS NULL OR u.config_group_id = '')
              )
              AND (
                (SELECT COUNT(*)
                 FROM links p
                 WHERE p.config_hash = l.config_hash
                   AND COALESCE(p.is_config_primary, 0) = 1
                ) != 1
                OR
                (SELECT MIN(id)
                 FROM links p
                 WHERE p.config_hash = l.config_hash
                   AND COALESCE(p.is_config_primary, 0) = 1
                ) IS NULL
                OR
                (SELECT MIN(id)
                 FROM links p
                 WHERE p.config_hash = l.config_hash
                   AND COALESCE(p.is_config_primary, 0) = 1
                ) !=
                (SELECT MIN(id)
                 FROM links m
                 WHERE m.config_hash = l.config_hash
                )
              )
            LIMIT ?
            """,
            (self.batch_size,),
        )
        rows = cur.fetchall()
        return [str(r["config_hash"]) for r in rows if r and r["config_hash"]]

    # -------------------------------
    # Core helpers
    # -------------------------------

    def _select_min_id_for_hash(self, cur: sqlite3.Cursor, h: str) -> Optional[int]:
        cur.execute(
            """
            SELECT MIN(id) AS min_id
            FROM links
            WHERE config_hash = ?
            """,
            (h,),
        )
        row = cur.fetchone()
        if not row or row["min_id"] is None:
            return None
        try:
            return int(row["min_id"])
        except Exception:
            return None

    def _find_existing_group_id_for_hash(self, cur: sqlite3.Cursor, h: str) -> Optional[str]:
        """
        group_id موجود (اگر هست) را برمی‌گرداند. (قدیمی‌ها را ری‌رایت نمی‌کنیم)
        """
        cur.execute(
            """
            SELECT config_group_id
            FROM links
            WHERE config_hash = ?
              AND config_group_id IS NOT NULL
              AND config_group_id != ''
            ORDER BY id
            LIMIT 1
            """,
            (h,),
        )
        row = cur.fetchone()
        if not row:
            return None
        gid = row["config_group_id"]
        if gid is None:
            return None
        gid_s = str(gid).strip()
        return gid_s or None

    def _fill_missing_group_id_only(self, cur: sqlite3.Cursor, h: str, group_id: str) -> int:
        """
        فقط NULL/'' ها را پر می‌کند. group_id های موجود را دست نمی‌زند.
        """
        cur.execute(
            """
            UPDATE links
            SET config_group_id = ?
            WHERE config_hash = ?
              AND (config_group_id IS NULL OR config_group_id = '')
            """,
            (group_id, h),
        )
        return int(cur.rowcount or 0)

    def _get_primary_state(self, cur: sqlite3.Cursor, h: str) -> Dict[str, Optional[int]]:
        """
        primary_count, primary_min_id (min id among primary), min_id (min id overall)
        """
        cur.execute(
            """
            SELECT
              (SELECT MIN(id) FROM links m WHERE m.config_hash = l.config_hash) AS min_id,
              (SELECT COUNT(*) FROM links p
                 WHERE p.config_hash = l.config_hash
                   AND COALESCE(p.is_config_primary, 0) = 1
              ) AS primary_count,
              (SELECT MIN(id) FROM links p
                 WHERE p.config_hash = l.config_hash
                   AND COALESCE(p.is_config_primary, 0) = 1
              ) AS primary_min_id
            FROM links l
            WHERE l.config_hash = ?
            LIMIT 1
            """,
            (h,),
        )
        row = cur.fetchone()
        if not row:
            return {"min_id": None, "primary_count": None, "primary_min_id": None}
        try:
            min_id = int(row["min_id"]) if row["min_id"] is not None else None
        except Exception:
            min_id = None
        try:
            primary_count = int(row["primary_count"]) if row["primary_count"] is not None else None
        except Exception:
            primary_count = None
        try:
            primary_min_id = int(row["primary_min_id"]) if row["primary_min_id"] is not None else None
        except Exception:
            primary_min_id = None
        return {"min_id": min_id, "primary_count": primary_count, "primary_min_id": primary_min_id}

    def _enforce_primary_min_id(self, cur: sqlite3.Cursor, h: str, min_id: int) -> int:
        """
        اولی (min_id) را primary می‌کند و بقیه را 0.
        """
        cur.execute(
            """
            UPDATE links
            SET is_config_primary = CASE WHEN id = ? THEN 1 ELSE 0 END
            WHERE config_hash = ?
            """,
            (min_id, h),
        )
        return int(cur.rowcount or 0)

    # -------------------------------
    # Processors
    # -------------------------------

    def _process_hash_grouping(self, cur: sqlite3.Cursor, h: str) -> None:
        min_id = self._select_min_id_for_hash(cur, h)
        if min_id is None:
            return

        group_id = self._find_existing_group_id_for_hash(cur, h)
        created = False
        if group_id is None:
            group_id = str(min_id)
            created = True

        grouped_rows = self._fill_missing_group_id_only(cur, h, group_id)
        if grouped_rows > 0:
            self.stats["rows_grouped"] += grouped_rows
        self.stats["hashes_grouped"] += 1
        if created:
            self.stats["groups_created"] += 1

        # برای همین hash هم primary را درست کن (کم‌هزینه و مطابق انتظار)
        st = self._get_primary_state(cur, h)
        if st["min_id"] is None:
            return
        if st["primary_count"] == 1 and st["primary_min_id"] == st["min_id"]:
            return
        affected = self._enforce_primary_min_id(cur, h, int(st["min_id"]))
        self.stats["hashes_primary_fixed"] += 1
        self.stats["rows_primary_fixed"] += affected

    def _process_hash_primary_fix_only(self, cur: sqlite3.Cursor, h: str) -> None:
        st = self._get_primary_state(cur, h)
        if st["min_id"] is None:
            return
        if st["primary_count"] == 1 and st["primary_min_id"] == st["min_id"]:
            return
        affected = self._enforce_primary_min_id(cur, h, int(st["min_id"]))
        self.stats["hashes_primary_fixed"] += 1
        self.stats["rows_primary_fixed"] += affected

    # -------------------------------
    # Main loop
    # -------------------------------

    def update_groups(self) -> None:
        conn = self._get_connection()
        try:
            cur = conn.cursor()

            # Phase 1: فقط new/partial groups -> group_id fill + primary enforce
            batch_no = 0
            while not self._stop_requested:
                hashes = self._fetch_hashes_needing_group(cur)
                if not hashes:
                    break
                batch_no += 1
                self.stats["batches_group"] += 1
                print(f"[group_updater] group phase batch={batch_no} hashes={len(hashes)}")

                conn.execute("BEGIN IMMEDIATE")
                try:
                    for h in hashes:
                        if self._stop_requested:
                            break
                        if not h:
                            continue
                        self._process_hash_grouping(cur, h)
                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise

            # Phase 2: فقط groups کامل -> اگر اولی primary نیست، اصلاح
            batch_no = 0
            while not self._stop_requested:
                hashes = self._fetch_hashes_needing_primary_fix(cur)
                if not hashes:
                    break
                batch_no += 1
                self.stats["batches_primary"] += 1
                print(f"[group_updater] primary phase batch={batch_no} hashes={len(hashes)}")

                conn.execute("BEGIN IMMEDIATE")
                try:
                    for h in hashes:
                        if self._stop_requested:
                            break
                        if not h:
                            continue
                        self._process_hash_primary_fix_only(cur, h)
                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise

            print(
                "[group_updater] done. "
                f"batches_group={self.stats['batches_group']}, "
                f"hashes_grouped={self.stats['hashes_grouped']}, "
                f"rows_grouped={self.stats['rows_grouped']}, "
                f"groups_created={self.stats['groups_created']}, "
                f"batches_primary={self.stats['batches_primary']}, "
                f"hashes_primary_fixed={self.stats['hashes_primary_fixed']}, "
                f"rows_primary_fixed={self.stats['rows_primary_fixed']}"
            )
        finally:
            conn.close()


def main() -> None:
    updater = ConfigGroupUpdater(batch_size=500)
    updater.update_groups()
    print("[group_updater] stats:", updater.stats)


if __name__ == "__main__":
    main()
