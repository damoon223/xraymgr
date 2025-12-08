# app/xraymgr/schema.py

import sqlite3
from .settings import get_db_path


def init_db_schema() -> None:
    """
    ساخت/تأیید اسکیمای دیتابیس:
      - جدول subscriptions
      - جدول links
    اگر جدول‌ها از قبل وجود داشته باشند، تغییری نمی‌کند.
    """
    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()

        # جدول subscriptions
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

        # جدول links
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS links (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL UNIQUE,
                config_json TEXT,
                config_hash VARCHAR(64),
                is_config_primary INTEGER,
                parent_id INTEGER,
                test_stage INTEGER NOT NULL DEFAULT 0,
                is_alive INTEGER NOT NULL DEFAULT 0,
                ip TEXT,
                country TEXT,
                city TEXT,
                datacenter TEXT,
                is_in_use INTEGER NOT NULL DEFAULT 0,
                bound_port INTEGER,
                last_test_at DATETIME,
                needs_replace INTEGER NOT NULL DEFAULT 0,
                is_invalid INTEGER NOT NULL DEFAULT 0
            )
            """
        )

        conn.commit()
    finally:
        conn.close()
