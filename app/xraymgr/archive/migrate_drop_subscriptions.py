#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import sqlite3
import sys
from pathlib import Path
from typing import List, Tuple


def _ensure_import_path() -> None:
    """
    Allow running this file directly from repo root:
      python app/xraymgr/migrate_drop_subscriptions.py
    by ensuring 'app/' is on sys.path.
    """
    app_dir = Path(__file__).resolve().parents[1]  # .../app
    app_dir_str = str(app_dir)
    if app_dir_str not in sys.path:
        sys.path.insert(0, app_dir_str)


def _default_db_path() -> str:
    _ensure_import_path()
    try:
        from xraymgr.settings import get_db_path  # type: ignore
        return str(get_db_path())
    except Exception:
        # Fallback: repo_root/data/xraymgr.db
        base_dir = Path(__file__).resolve().parents[2]
        data_dir = base_dir / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        return str(data_dir / "xraymgr.db")


def _list_tables(conn: sqlite3.Connection) -> List[str]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type='table' AND name NOT LIKE 'sqlite_%'
        ORDER BY name
        """
    )
    return [str(r[0]) for r in cur.fetchall()]


def _find_fk_referrers(conn: sqlite3.Connection, target_table: str) -> List[Tuple[str, str, str, str]]:
    """
    Returns tuples of (table, from_col, ref_table, ref_col) for FKs that reference target_table.
    """
    referrers: List[Tuple[str, str, str, str]] = []
    cur = conn.cursor()
    for tbl in _list_tables(conn):
        try:
            cur.execute(f"PRAGMA foreign_key_list({tbl})")
            for row in cur.fetchall():
                # row columns: (id, seq, table, from, to, on_update, on_delete, match)
                ref_table = str(row[2])
                if ref_table == target_table:
                    referrers.append((tbl, str(row[3]), ref_table, str(row[4])))
        except sqlite3.Error:
            continue
    return referrers


def _find_sql_dependents(conn: sqlite3.Connection, needle: str) -> List[Tuple[str, str]]:
    """
    Best-effort: find objects whose SQL text contains the needle (case-insensitive).
    Returns (type, name).
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT type, name, sql
        FROM sqlite_master
        WHERE sql IS NOT NULL
          AND type IN ('view','trigger','index')
        """
    )
    out: List[Tuple[str, str]] = []
    n = needle.lower()
    for t, name, sql in cur.fetchall():
        if sql and n in str(sql).lower():
            out.append((str(t), str(name)))
    out.sort()
    return out


def drop_subscriptions(db_path: str, *, force: bool) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA foreign_keys=ON;" if not force else "PRAGMA foreign_keys=OFF;")
        cur = conn.cursor()

        # Preflight diagnostics (only used if drop fails)
        fk_referrers = _find_fk_referrers(conn, "subscriptions")
        sql_deps = _find_sql_dependents(conn, "subscriptions")

        cur.execute("BEGIN;")
        try:
            cur.execute("DROP TABLE IF EXISTS subscriptions;")
        except sqlite3.Error as e:
            cur.execute("ROLLBACK;")
            msg = [f"FAILED: {e}"]

            if fk_referrers:
                msg.append("Foreign-key referrers detected (table.from -> subscriptions.to):")
                for tbl, from_col, _, to_col in fk_referrers:
                    msg.append(f"  - {tbl}.{from_col} -> subscriptions.{to_col}")

            if sql_deps:
                msg.append("Other schema objects mentioning 'subscriptions' in SQL:")
                for t, name in sql_deps:
                    msg.append(f"  - {t}: {name}")

            raise RuntimeError("\n".join(msg)) from e

        cur.execute("COMMIT;")

    finally:
        conn.close()


def main() -> None:
    p = argparse.ArgumentParser(description="Drop the 'subscriptions' table from xraymgr SQLite DB.")
    p.add_argument("--db", default=_default_db_path(), help="Path to sqlite DB file.")
    p.add_argument(
        "--force",
        action="store_true",
        help="Disable FK enforcement for this connection before dropping (unsafe if other tables reference subscriptions).",
    )
    args = p.parse_args()

    drop_subscriptions(str(args.db), force=bool(args.force))
    print(f"OK: dropped table 'subscriptions' in DB: {args.db}")


if __name__ == "__main__":
    main()
