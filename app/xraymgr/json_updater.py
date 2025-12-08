import sqlite3
import threading
from typing import Dict, List

from .settings import get_db_path
from .jsbridge import NodeLinkConverter, NodeBridgeError


class JsonUpdaterStopped(Exception):
    """Signal برای توقف graceful JSON updater."""
    pass


class JsonConfigUpdater:
    """
    پر کردن ستون config_json در جدول links:
      - فقط ردیف‌هایی که:
          * config_json IS NULL
          * url IS NOT NULL
          * is_invalid = 0
      - پردازش در batchهای محدود (مثلاً 1000 ردیف)
      - Dedup بر اساس URL، هر URL فقط یک بار به Node داده می‌شود
      - اگر یک URL به JSON تبدیل نشود → همهٔ ردیف‌های آن URL is_invalid = 1
    """

    def __init__(self, batch_size: int = 1000, node_timeout: float = 15.0) -> None:
        self.batch_size = batch_size
        self.node_timeout = node_timeout

        self.stats = {
            "total_candidates": 0,
            "batches": 0,
            "urls_seen": 0,
            "urls_converted": 0,
            "urls_failed": 0,
            "rows_updated": 0,  # شامل آپدیت config_json و is_invalid
        }

        self._stop_event = threading.Event()
        self._stats_lock = threading.Lock()

    def request_stop(self) -> None:
        print("[json_updater] stop requested")
        self._stop_event.set()

    def _check_stopped(self) -> None:
        if self._stop_event.is_set():
            raise JsonUpdaterStopped()

    def update_missing_json(self) -> None:
        print(
            f"[json_updater] starting JSON update job (batch_size={self.batch_size}, "
            f"node_timeout={self.node_timeout:.1f}s)"
        )

        conn = sqlite3.connect(get_db_path())
        conn.row_factory = sqlite3.Row

        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute("PRAGMA temp_store = MEMORY;")

        try:
            with NodeLinkConverter(timeout=self.node_timeout) as converter:
                print("[json_updater] NodeLinkConverter initialized.")
                last_id = 0

                while True:
                    self._check_stopped()

                    cur = conn.cursor()
                    cur.execute(
                        """
                        SELECT id, url
                        FROM links
                        WHERE config_json IS NULL
                          AND url IS NOT NULL
                          AND is_invalid = 0
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

                    batch_count = len(rows)
                    with self._stats_lock:
                        self.stats["batches"] += 1
                        self.stats["total_candidates"] += batch_count

                    print(
                        f"[json_updater] batch loaded: {batch_count} rows "
                        f"(last_id before batch={last_id})"
                    )

                    # map: url -> [ids...]
                    url_to_ids: Dict[str, List[int]] = {}
                    for row in rows:
                        self._check_stopped()
                        url = row["url"]
                        row_id = int(row["id"])
                        last_id = row_id
                        if not url:
                            continue
                        url = url.strip()
                        if not url:
                            continue
                        url_to_ids.setdefault(url, []).append(row_id)

                    distinct_urls = len(url_to_ids)
                    with self._stats_lock:
                        self.stats["urls_seen"] += distinct_urls

                    print(
                        f"[json_updater] processing {distinct_urls} distinct URLs "
                        f"for this batch..."
                    )

                    updates: List[tuple] = []       # (json_text, id)
                    invalid_ids: List[int] = []     # idهایی که باید is_invalid=1 شوند

                    for idx, (url, ids) in enumerate(url_to_ids.items(), start=1):
                        self._check_stopped()
                        url_preview = (url[:80] + "…") if len(url) > 80 else url
                        try:
                            json_text = converter.convert(url)
                        except NodeBridgeError as e:
                            print(
                                f"[json_updater] NodeBridgeError for url[{idx}/{distinct_urls}]: "
                                f"{url_preview} :: {e}"
                            )
                            json_text = None
                        except Exception as e:
                            print(
                                f"[json_updater] ERROR converting url[{idx}/{distinct_urls}]: "
                                f"{url_preview} :: {e}"
                            )
                            json_text = None

                        # اگر چیزی برنگشت یا null بود → این URL نامعتبر است
                        if not json_text:
                            with self._stats_lock:
                                self.stats["urls_failed"] += 1
                            invalid_ids.extend(ids)
                            continue

                        json_stripped = json_text.strip()
                        if not json_stripped or json_stripped.lower() == "null":
                            with self._stats_lock:
                                self.stats["urls_failed"] += 1
                            invalid_ids.extend(ids)
                            continue

                        # موفق
                        with self._stats_lock:
                            self.stats["urls_converted"] += 1

                        for row_id in ids:
                            updates.append((json_text, row_id))

                    if not updates and not invalid_ids:
                        print("[json_updater] no valid JSON results and no invalids in this batch.")
                        continue

                    try:
                        cur = conn.cursor()
                        cur.execute("BEGIN")

                        if updates:
                            cur.executemany(
                                "UPDATE links SET config_json = ? WHERE id = ?",
                                updates,
                            )

                        if invalid_ids:
                            cur.executemany(
                                "UPDATE links SET is_invalid = 1 WHERE id = ?",
                                [(i,) for i in invalid_ids],
                            )

                        conn.commit()
                        # rowcount در SQLite فقط برای آخرین statement معتبر است،
                        # ولی برای آمار کلی همین هم بد نیست.
                        rows_affected = cur.rowcount
                    except Exception as e:
                        conn.rollback()
                        print(f"[json_updater] ERROR committing batch update: {e}")
                        rows_affected = 0

                    with self._stats_lock:
                        self.stats["rows_updated"] += max(rows_affected, 0)

                    print(
                        f"[json_updater] batch updated:"
                        f" config_updates={len(updates)}, "
                        f"invalid_marked={len(invalid_ids)}, "
                        f"sqlite rowcount(last stmt)={rows_affected}, "
                        f"rows_updated_total={self.stats['rows_updated']}"
                    )

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
                f"\n   total candidate rows: {self.stats['total_candidates']}"
                f"\n   batches:             {self.stats['batches']}"
                f"\n   urls seen:           {self.stats['urls_seen']}"
                f"\n   urls converted:      {self.stats['urls_converted']}"
                f"\n   urls failed:         {self.stats['urls_failed']}"
                f"\n   rows updated(last-stmt sum): {self.stats['rows_updated']}"
            )


if __name__ == "__main__":
    updater = JsonConfigUpdater(batch_size=1000)
    updater.update_missing_json()
