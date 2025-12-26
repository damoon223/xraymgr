import sqlite3
from typing import Dict, List

from .settings import get_db_path


def _table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    # (cid, name, type, notnull, dflt, pk)
    return [str(r[1]) for r in cur.fetchall()]


def init_db_schema() -> None:
    """
    ساخت/تأیید اسکیمای دیتابیس (SQLite).

    نکته مهم:
    - CREATE TABLE IF NOT EXISTS اسکیمای DBهای قدیمی را تغییر نمی‌دهد.
    - برای پایدار شدن jobها روی DBهای قدیمی‌تر، این تابع تلاش می‌کند بعضی ستون‌های لازم
      (مثل is_invalid / is_protocol_unsupported / parent_id / repaired_url) را اگر نبودند،
      با ALTER TABLE اضافه کند.
    """
    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS subscriptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL UNIQUE,
                is_valid INTEGER NOT NULL DEFAULT 1,
                last_sync_at DATETIME,
                content TEXT,
                content_hash VARCHAR(64)
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS links (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL UNIQUE,
                config_json TEXT,
                config_hash VARCHAR(64),
                is_config_primary INTEGER,
                test_stage INTEGER NOT NULL DEFAULT 0,
                is_alive INTEGER NOT NULL DEFAULT 1,
                ip TEXT,
                country TEXT,
                city TEXT,
                datacenter TEXT,
                is_in_use INTEGER NOT NULL DEFAULT 0,
                bound_port INTEGER,
                last_test_at DATETIME,
                needs_replace INTEGER NOT NULL DEFAULT 0,

                parent_id INTEGER,
                is_invalid INTEGER NOT NULL DEFAULT 0,
                is_protocol_unsupported INTEGER NOT NULL DEFAULT 0,
                repaired_url TEXT
            )
            """
        )

        cols = set(_table_columns(conn, "links"))

        wanted: Dict[str, str] = {
            "parent_id": "ALTER TABLE links ADD COLUMN parent_id INTEGER",
            "is_invalid": "ALTER TABLE links ADD COLUMN is_invalid INTEGER NOT NULL DEFAULT 0",
            "is_protocol_unsupported": "ALTER TABLE links ADD COLUMN is_protocol_unsupported INTEGER NOT NULL DEFAULT 0",
            "repaired_url": "ALTER TABLE links ADD COLUMN repaired_url TEXT",
        }

        for name, stmt in wanted.items():
            if name in cols:
                continue
            try:
                cur.execute(stmt)
            except sqlite3.Error as e:
                print(f"[schema] WARN: could not add column {name}: {e}")

        conn.commit()
    finally:
        conn.close()
