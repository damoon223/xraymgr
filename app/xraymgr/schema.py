import sqlite3
from typing import Dict, List

from .settings import get_db_path


def _table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    return [str(r[1]) for r in cur.fetchall()]


def init_db_schema() -> None:
    """
    ساخت/تأیید اسکیمای دیتابیس (SQLite).

    نکته مهم:
    - CREATE TABLE IF NOT EXISTS اسکیمای DBهای قدیمی را تغییر نمی‌دهد.
    - برای پایدار شدن jobها روی DBهای قدیمی‌تر، این تابع ستون‌های لازم را اگر نبودند، با ALTER TABLE اضافه می‌کند.
    - همچنین indexهای لازم (partial/unique) را ایجاد می‌کند.

    تغییرات این نسخه:
    - حذف/عدم ساخت idx_inbound_role_unique (role باید non-unique باشد تا چند inbound تست/اصلی همزمان داشته باشیم)
    - سازگار کردن CREATE TABLE inbound با nullable بودن link_id و outbound_tag (slotهای تست آزاد)
    - اضافه کردن ستون‌های lock/state/result برای links به‌صورت idempotent
    """

    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()

        # links (outbounds)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS links (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              url TEXT NOT NULL UNIQUE,
              config_json TEXT,
              config_hash VARCHAR(64),
              is_config_primary INTEGER,
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
              is_invalid INTEGER NOT NULL DEFAULT 0,
              config_group_id TEXT DEFAULT '',
              is_protocol_unsupported INTEGER NOT NULL DEFAULT 0,
              parent_id INTEGER,
              repaired_url TEXT,
              outbound_tag TEXT,
              inbound_tag TEXT
            )
            """
        )

        # inbound
        # NOTE: link_id و outbound_tag عمداً nullable هستند تا slotهای تست "آزاد" معنی داشته باشند.
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS inbound (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              role TEXT NOT NULL CHECK (role IN ('primary','test')),
              is_active INTEGER NOT NULL DEFAULT 0 CHECK (is_active IN (0,1)),
              port INTEGER NOT NULL,
              tag TEXT NOT NULL,
              link_id INTEGER REFERENCES links(id) ON DELETE RESTRICT,
              outbound_tag TEXT,
              status TEXT NOT NULL DEFAULT 'new' CHECK (TRIM(status) <> '' AND instr(status,' ') = 0),
              last_test_at DATETIME
            )
            """
        )

        # Migrate older DBs: add missing columns (idempotent)
        cols = set(_table_columns(conn, "links"))
        wanted: Dict[str, str] = {
            "config_json": "ALTER TABLE links ADD COLUMN config_json TEXT",
            "config_hash": "ALTER TABLE links ADD COLUMN config_hash VARCHAR(64)",
            "is_config_primary": "ALTER TABLE links ADD COLUMN is_config_primary INTEGER",
            "test_stage": "ALTER TABLE links ADD COLUMN test_stage INTEGER NOT NULL DEFAULT 0",
            "is_alive": "ALTER TABLE links ADD COLUMN is_alive INTEGER NOT NULL DEFAULT 0",
            "ip": "ALTER TABLE links ADD COLUMN ip TEXT",
            "country": "ALTER TABLE links ADD COLUMN country TEXT",
            "city": "ALTER TABLE links ADD COLUMN city TEXT",
            "datacenter": "ALTER TABLE links ADD COLUMN datacenter TEXT",
            "is_in_use": "ALTER TABLE links ADD COLUMN is_in_use INTEGER NOT NULL DEFAULT 0",
            "bound_port": "ALTER TABLE links ADD COLUMN bound_port INTEGER",
            "last_test_at": "ALTER TABLE links ADD COLUMN last_test_at DATETIME",
            "needs_replace": "ALTER TABLE links ADD COLUMN needs_replace INTEGER NOT NULL DEFAULT 0",
            "is_invalid": "ALTER TABLE links ADD COLUMN is_invalid INTEGER NOT NULL DEFAULT 0",
            "config_group_id": "ALTER TABLE links ADD COLUMN config_group_id TEXT DEFAULT ''",
            "is_protocol_unsupported": "ALTER TABLE links ADD COLUMN is_protocol_unsupported INTEGER NOT NULL DEFAULT 0",
            "parent_id": "ALTER TABLE links ADD COLUMN parent_id INTEGER",
            "repaired_url": "ALTER TABLE links ADD COLUMN repaired_url TEXT",
            "outbound_tag": "ALTER TABLE links ADD COLUMN outbound_tag TEXT",
            "inbound_tag": "ALTER TABLE links ADD COLUMN inbound_tag TEXT",
            # test lock/state (batch testing)
            "test_status": "ALTER TABLE links ADD COLUMN test_status TEXT NOT NULL DEFAULT 'idle'",
            "test_started_at": "ALTER TABLE links ADD COLUMN test_started_at DATETIME",
            "test_lock_until": "ALTER TABLE links ADD COLUMN test_lock_until DATETIME",
            "test_lock_owner": "ALTER TABLE links ADD COLUMN test_lock_owner TEXT",
            "test_batch_id": "ALTER TABLE links ADD COLUMN test_batch_id TEXT",
            # test results
            "last_test_ok": "ALTER TABLE links ADD COLUMN last_test_ok INTEGER NOT NULL DEFAULT 0",
            "last_test_error": "ALTER TABLE links ADD COLUMN last_test_error TEXT",
        }

        for name, stmt in wanted.items():
            if name in cols:
                continue
            try:
                cur.execute(stmt)
            except sqlite3.Error as e:
                print(f"[schema] WARN: could not add column {name}: {e}")

        # indexes
        try:
            cur.execute("CREATE INDEX IF NOT EXISTS idx_links_config_hash ON links(config_hash)")
        except sqlite3.Error as e:
            print(f"[schema] WARN: could not create index for config_hash: {e}")

        # Test lock/state indexes
        try:
            cur.execute("CREATE INDEX IF NOT EXISTS idx_links_test_status ON links(test_status)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_links_test_lock_until ON links(test_lock_until)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_links_test_batch_id ON links(test_batch_id)")
        except sqlite3.Error as e:
            print(f"[schema] WARN: could not create test-lock indexes: {e}")

        # Unique indexes for tags (partial: ignore NULL/empty)
        try:
            cur.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_links_outbound_tag_unique
                ON links(outbound_tag)
                WHERE outbound_tag IS NOT NULL AND TRIM(outbound_tag) <> ''
                """
            )
        except sqlite3.Error as e:
            print(f"[schema] WARN: could not create unique index for outbound_tag: {e}")

        try:
            cur.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_links_inbound_tag_unique
                ON links(inbound_tag)
                WHERE inbound_tag IS NOT NULL AND TRIM(inbound_tag) <> ''
                """
            )
        except sqlite3.Error as e:
            print(f"[schema] WARN: could not create unique index for inbound_tag: {e}")

        # inbound indexes
        try:
            # اگر از قبل (در DBهای قدیمی) unique بوده، حذفش می‌کنیم تا چند inbound برای هر role ممکن باشد.
            cur.execute("DROP INDEX IF EXISTS idx_inbound_role_unique")

            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_inbound_port_unique ON inbound(port)")
            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_inbound_tag_unique ON inbound(tag)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_inbound_link_id ON inbound(link_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_inbound_out_tag ON inbound(outbound_tag)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_inbound_role ON inbound(role)")
        except sqlite3.Error as e:
            print(f"[schema] WARN: could not create inbound indexes: {e}")

        conn.commit()
    finally:
        conn.close()
