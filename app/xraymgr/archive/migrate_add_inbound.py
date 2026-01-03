#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import sqlite3
import sys
from pathlib import Path
from typing import Dict, List


def _ensure_import_path() -> None:
    """
    Allow running this file directly from repo root:
      python app/xraymgr/migrate_add_inbound.py
    by ensuring 'app/' is on sys.path.
    """
    app_dir = Path(__file__).resolve().parents[1]  # .../app
    app_dir_str = str(app_dir)
    if app_dir_str not in sys.path:
        sys.path.insert(0, app_dir_str)


def _default_db_path() -> str:
    """
    Prefer project settings.get_db_path(); fallback to repo layout.
    """
    _ensure_import_path()
    try:
        from xraymgr.settings import get_db_path  # type: ignore
        return str(get_db_path())
    except Exception:
        base_dir = Path(__file__).resolve().parents[2]  # repo root
        data_dir = base_dir / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        return str(data_dir / "xraymgr.db")


def _table_columns(cur: sqlite3.Cursor, table: str) -> List[str]:
    cur.execute(f"PRAGMA table_info({table})")
    return [str(r[1]) for r in cur.fetchall()]


def _ensure_inbound_table(cur: sqlite3.Cursor) -> None:
    # Table create (idempotent)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS inbound (
          id           INTEGER PRIMARY KEY AUTOINCREMENT,

          role         TEXT NOT NULL
                         CHECK (role IN ('primary','test')),

          is_active    INTEGER NOT NULL DEFAULT 0
                         CHECK (is_active IN (0,1)),

          port         INTEGER NOT NULL,
          tag          TEXT NOT NULL,

          link_id      INTEGER NOT NULL
                         REFERENCES links(id) ON DELETE RESTRICT,

          outbound_tag TEXT NOT NULL,

          status       TEXT NOT NULL DEFAULT 'new'
                         CHECK (TRIM(status) <> '' AND instr(status,' ') = 0),

          last_test_at DATETIME
        )
        """
    )

    # If table existed from earlier experiments, add missing columns (best-effort)
    cols = set(_table_columns(cur, "inbound"))
    wanted: Dict[str, str] = {
        "role": "ALTER TABLE inbound ADD COLUMN role TEXT",
        "is_active": "ALTER TABLE inbound ADD COLUMN is_active INTEGER NOT NULL DEFAULT 0",
        "port": "ALTER TABLE inbound ADD COLUMN port INTEGER",
        "tag": "ALTER TABLE inbound ADD COLUMN tag TEXT",
        "link_id": "ALTER TABLE inbound ADD COLUMN link_id INTEGER",
        "outbound_tag": "ALTER TABLE inbound ADD COLUMN outbound_tag TEXT",
        "status": "ALTER TABLE inbound ADD COLUMN status TEXT NOT NULL DEFAULT 'new'",
        "last_test_at": "ALTER TABLE inbound ADD COLUMN last_test_at DATETIME",
    }
    for name, stmt in wanted.items():
        if name in cols:
            continue
        try:
            cur.execute(stmt)
        except sqlite3.Error:
            # ignore: schema may be incompatible / column constraints may differ
            pass


def _ensure_inbound_indexes(cur: sqlite3.Cursor) -> None:
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_inbound_role_unique ON inbound(role)")
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_inbound_port_unique ON inbound(port)")
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_inbound_tag_unique  ON inbound(tag)")
    cur.execute("CREATE INDEX        IF NOT EXISTS idx_inbound_link_id     ON inbound(link_id)")
    cur.execute("CREATE INDEX        IF NOT EXISTS idx_inbound_out_tag     ON inbound(outbound_tag)")


def migrate(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA foreign_keys=ON;")
        cur = conn.cursor()

        _ensure_inbound_table(cur)
        _ensure_inbound_indexes(cur)

        conn.commit()
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Create/migrate inbound table for xraymgr SQLite DB.")
    parser.add_argument("--db", default=_default_db_path(), help="Path to sqlite DB file (default: project data/xraymgr.db).")
    args = parser.parse_args()

    migrate(str(args.db))
    print(f"OK: ensured table 'inbound' in DB: {args.db}")


if __name__ == "__main__":
    main()
