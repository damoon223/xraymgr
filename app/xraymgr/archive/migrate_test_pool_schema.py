#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SQLite migration for xraymgr to support:
- Multiple inbound rows per role (drop UNIQUE on inbound(role) and create a normal index instead)
- Test inbound "slots" by allowing inbound.link_id and inbound.outbound_tag to be NULL
- Atomic test locking/state fields on links (outbounds)
- Test result fields on links

Usage:
  python3 app/xraymgr/archive/migrate_test_pool_schema.py --db /opt/xraymgr/data/xraymgr.db

Notes:
- SQLite cannot ALTER COLUMN to drop NOT NULL; inbound table is rebuilt when needed.
- Your app currently RE-CREATES idx_inbound_role_unique in app/xraymgr/schema.py (init_db_schema).
  After running this migration, you must remove/disable that UNIQUE index creation in schema.py,
  otherwise it will be created again at startup.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import os
import shutil
import sqlite3
import sys
from typing import Dict, Tuple


def _utc_ts() -> str:
    return _dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")


def backup_db(db_path: str) -> str:
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"DB not found: {db_path}")
    backup_path = f"{db_path}.bak_{_utc_ts()}"
    shutil.copy2(db_path, backup_path)
    return backup_path


def table_info(conn: sqlite3.Connection, table: str) -> Dict[str, Dict[str, object]]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    out: Dict[str, Dict[str, object]] = {}
    for cid, name, col_type, notnull, dflt_value, pk in cur.fetchall():
        out[str(name)] = {
            "type": str(col_type),
            "notnull": int(notnull),
            "default": dflt_value,
            "pk": int(pk),
        }
    return out


def index_exists(conn: sqlite3.Connection, index_name: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type='index' AND name=? LIMIT 1",
        (index_name,),
    )
    return cur.fetchone() is not None


def column_exists(conn: sqlite3.Connection, table: str, col: str) -> bool:
    return col in table_info(conn, table)


def add_column_if_missing(conn: sqlite3.Connection, table: str, col: str, ddl: str) -> None:
    if column_exists(conn, table, col):
        print(f"[skip] {table}.{col} already exists")
        return
    print(f"[apply] {ddl}")
    conn.execute(ddl)


def create_index_if_missing(conn: sqlite3.Connection, index_name: str, ddl: str) -> None:
    if index_exists(conn, index_name):
        print(f"[skip] index {index_name} already exists")
        return
    print(f"[apply] {ddl}")
    conn.execute(ddl)


def inbound_needs_rebuild(conn: sqlite3.Connection) -> Tuple[bool, str]:
    cols = table_info(conn, "inbound")
    if "link_id" not in cols or "outbound_tag" not in cols:
        return True, "inbound missing required columns"
    if int(cols["link_id"]["notnull"]) == 1:
        return True, "inbound.link_id is NOT NULL"
    if int(cols["outbound_tag"]["notnull"]) == 1:
        return True, "inbound.outbound_tag is NOT NULL"
    return False, "inbound columns already nullable"


def rebuild_inbound_make_nullable(conn: sqlite3.Connection) -> None:
    """
    Rebuild inbound with the SAME columns/constraints as current_schema.sql,
    except:
      - link_id is nullable
      - outbound_tag is nullable
      - idx_inbound_role_unique is NOT created; instead idx_inbound_role (non-unique) is created
    """
    script = """
PRAGMA foreign_keys=OFF;
BEGIN;

ALTER TABLE inbound RENAME TO inbound__old;

CREATE TABLE inbound (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  role TEXT NOT NULL CHECK (role IN ('primary','test')),
  is_active INTEGER NOT NULL DEFAULT 0 CHECK (is_active IN (0,1)),
  port INTEGER NOT NULL,
  tag TEXT NOT NULL,
  link_id INTEGER REFERENCES links(id) ON DELETE RESTRICT,
  outbound_tag TEXT,
  status TEXT NOT NULL DEFAULT 'new' CHECK (TRIM(status) <> '' AND instr(status,' ') = 0),
  last_test_at DATETIME
);

INSERT INTO inbound(id, role, is_active, port, tag, link_id, outbound_tag, status, last_test_at)
SELECT id, role, is_active, port, tag, link_id, outbound_tag, status, last_test_at
FROM inbound__old;

DROP TABLE inbound__old;

-- indexes (keep the original ones, but remove role-unique and add role normal index)
CREATE INDEX IF NOT EXISTS idx_inbound_link_id ON inbound(link_id);
CREATE INDEX IF NOT EXISTS idx_inbound_out_tag ON inbound(outbound_tag);
CREATE UNIQUE INDEX IF NOT EXISTS idx_inbound_port_unique ON inbound(port);
CREATE UNIQUE INDEX IF NOT EXISTS idx_inbound_tag_unique ON inbound(tag);
CREATE INDEX IF NOT EXISTS idx_inbound_role ON inbound(role);

COMMIT;
PRAGMA foreign_keys=ON;
"""
    print("[apply] rebuild inbound (make link_id/outbound_tag nullable, drop role unique)")
    conn.executescript(script)


def drop_inbound_role_unique_index(conn: sqlite3.Connection) -> None:
    # If inbound was rebuilt, the unique index is gone; still safe to drop if present.
    if index_exists(conn, "idx_inbound_role_unique"):
        print("[apply] DROP INDEX idx_inbound_role_unique")
        conn.execute("DROP INDEX IF EXISTS idx_inbound_role_unique")
    else:
        print("[skip] idx_inbound_role_unique not present")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--db",
        default=os.getenv("XRAYMGR_DB_PATH", "data/xraymgr.db"),
        help="Path to SQLite DB file (default: data/xraymgr.db or XRAYMGR_DB_PATH)",
    )
    ap.add_argument(
        "--no-backup",
        action="store_true",
        help="Do not create a .bak_YYYYMMDD_HHMMSS copy before applying migrations",
    )
    args = ap.parse_args()

    db_path = args.db

    if not args.no_backup:
        bak = backup_db(db_path)
        print(f"[backup] {bak}")

    conn = sqlite3.connect(db_path)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")

        # 1) Drop UNIQUE index on inbound(role) (will be re-created by app unless you change schema.py)
        drop_inbound_role_unique_index(conn)

        # 2) Make inbound.link_id and inbound.outbound_tag nullable (requires rebuild in SQLite)
        needs, reason = inbound_needs_rebuild(conn)
        print(f"[check] inbound rebuild needed: {needs} ({reason})")
        if needs:
            rebuild_inbound_make_nullable(conn)
        else:
            # Ensure non-unique role index exists
            create_index_if_missing(conn, "idx_inbound_role", "CREATE INDEX idx_inbound_role ON inbound(role)")

        # 3) Add atomic test lock/state fields on links (outbounds)
        # Safer DDL: no CHECK constraints in ALTER TABLE (avoid compatibility surprises).
        add_column_if_missing(conn, "links", "test_status",
                              "ALTER TABLE links ADD COLUMN test_status TEXT NOT NULL DEFAULT 'idle'")
        add_column_if_missing(conn, "links", "test_started_at",
                              "ALTER TABLE links ADD COLUMN test_started_at DATETIME")
        add_column_if_missing(conn, "links", "test_lock_until",
                              "ALTER TABLE links ADD COLUMN test_lock_until DATETIME")
        add_column_if_missing(conn, "links", "test_lock_owner",
                              "ALTER TABLE links ADD COLUMN test_lock_owner TEXT")
        add_column_if_missing(conn, "links", "test_batch_id",
                              "ALTER TABLE links ADD COLUMN test_batch_id TEXT")

        create_index_if_missing(conn, "idx_links_test_status",
                                "CREATE INDEX idx_links_test_status ON links(test_status)")
        create_index_if_missing(conn, "idx_links_test_lock_until",
                                "CREATE INDEX idx_links_test_lock_until ON links(test_lock_until)")
        create_index_if_missing(conn, "idx_links_test_batch_id",
                                "CREATE INDEX idx_links_test_batch_id ON links(test_batch_id)")

        # 4) Test result fields
        # last_test_at already exists in links (per current_schema.sql); keep it.
        add_column_if_missing(conn, "links", "last_test_ok",
                              "ALTER TABLE links ADD COLUMN last_test_ok INTEGER NOT NULL DEFAULT 0")
        add_column_if_missing(conn, "links", "last_test_error",
                              "ALTER TABLE links ADD COLUMN last_test_error TEXT")

        conn.commit()

        print("\n[done] Migration applied successfully.")
        print("[important] Update app/xraymgr/schema.py: remove/disable creation of UNIQUE idx_inbound_role_unique,")
        print("           otherwise it will be created again at startup and break multiple test/main inbounds.")

        return 0

    except Exception as e:
        conn.rollback()
        print(f"\n[error] {e}", file=sys.stderr)
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
