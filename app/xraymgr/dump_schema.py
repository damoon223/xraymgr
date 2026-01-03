#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dump SQLite schema (DDL only) into a file.

Default output:
  app/xraymgr/current_schema.sql

- Uses the project's default DB path from settings.get_db_path() unless --db is provided.
- Optionally runs init_db_schema() first to ensure migrations/indices are present (default: enabled).
- Outputs deterministic, executable SQL (tables -> indexes -> triggers -> views).
- Does NOT dump data.

Examples:
  python -m xraymgr.dump_schema
  python -m xraymgr.dump_schema --out /opt/xraymgr/app/xraymgr/current_schema.sql
  python app/xraymgr/dump_schema.py --db /path/to/xraymgr.db --out ./current_schema.sql
"""

from __future__ import annotations

import argparse
import datetime as _dt
import os
import sqlite3
import sys
from pathlib import Path
from typing import Iterable, List, Optional, Tuple


def _bootstrap_imports():
    """
    Support both:
      - python -m xraymgr.dump_schema        (package context; relative imports work)
      - python app/xraymgr/dump_schema.py    (direct script; relative imports fail)
    """
    try:
        from .schema import init_db_schema as _init_db_schema
        from .settings import get_db_path as _get_db_path

        return _init_db_schema, _get_db_path
    except ImportError:
        # Running as a script: add the /app directory to sys.path and use absolute imports.
        pkg_dir = Path(__file__).resolve().parent          # .../app/xraymgr
        app_dir = pkg_dir.parent                          # .../app
        app_dir_str = str(app_dir)
        if app_dir_str not in sys.path:
            sys.path.insert(0, app_dir_str)

        from xraymgr.schema import init_db_schema as _init_db_schema  # type: ignore
        from xraymgr.settings import get_db_path as _get_db_path      # type: ignore

        return _init_db_schema, _get_db_path


init_db_schema, get_db_path = _bootstrap_imports()

_TYPE_ORDER = {
    "table": 1,
    "index": 2,
    "trigger": 3,
    "view": 4,
}


def _ensure_semicolon(sql: str) -> str:
    s = (sql or "").strip()
    if not s:
        return s
    return s if s.endswith(";") else s + ";"


def _is_internal_sqlite_object(name: str) -> bool:
    n = (name or "").strip().lower()
    return n.startswith("sqlite_")


def _fetch_master(conn: sqlite3.Connection) -> List[Tuple[str, str, str, str]]:
    """
    Returns rows: (type, name, tbl_name, sql)
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT type, name, tbl_name, sql
        FROM sqlite_master
        WHERE sql IS NOT NULL
          AND type IN ('table','index','trigger','view')
        """
    )

    rows: List[Tuple[str, str, str, str]] = []
    for t, name, tbl, sql in cur.fetchall():
        if _is_internal_sqlite_object(name):
            continue
        rows.append((str(t), str(name), str(tbl), str(sql)))

    rows.sort(key=lambda r: (_TYPE_ORDER.get(r[0], 99), r[1].lower()))
    return rows


def _pragma_table_info(conn: sqlite3.Connection, table: str) -> List[Tuple[int, str, str, int, Optional[str], int]]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    # (cid, name, type, notnull, dflt, pk)
    return cur.fetchall()


def _pragma_index_list(conn: sqlite3.Connection, table: str) -> List[Tuple[int, str, int, str, int]]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA index_list({table})")
    # (seq, name, unique, origin, partial)
    return cur.fetchall()


def _format_schema_sql(
    conn: sqlite3.Connection,
    *,
    db_path: str,
    out_path: str,
    include_pragmas_as_comments: bool,
) -> List[str]:
    rows = _fetch_master(conn)
    tables = [r for r in rows if r[0] == "table"]
    indexes = [r for r in rows if r[0] == "index"]
    triggers = [r for r in rows if r[0] == "trigger"]
    views = [r for r in rows if r[0] == "view"]

    now = _dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    out: List[str] = []
    out.append("-- This file is the CURRENT SQLite schema for the xraymgr project.")
    out.append("-- It is intentionally committed as a reference snapshot for future sessions.")
    out.append("-- Regenerate it by running: app/xraymgr/dump_schema.py (or: python -m xraymgr.dump_schema)")
    out.append("--")
    out.append(f"-- Generated at (UTC): {now}")
    out.append(f"-- DB: {db_path}")
    out.append(f"-- Output: {out_path}")
    out.append("-- ===================================================================")
    out.append("")
    out.append("PRAGMA foreign_keys=OFF;")
    out.append("BEGIN TRANSACTION;")
    out.append("")

    def emit_section(title: str, items: Iterable[Tuple[str, str, str, str]]) -> None:
        nonlocal out
        out.append(f"-- ---- {title} ----")
        for t, name, tbl, sql in items:
            if include_pragmas_as_comments and t == "table":
                out.append(f"-- table: {name}")
                try:
                    cols = _pragma_table_info(conn, name)
                    for cid, col_name, col_type, notnull, dflt, pk in cols:
                        out.append(
                            f"-- col[{cid}]: {col_name} {col_type}"
                            f"{' NOT NULL' if notnull else ''}"
                            f"{' DEFAULT ' + str(dflt) if dflt is not None else ''}"
                            f"{' PK' if pk else ''}"
                        )
                    idxs = _pragma_index_list(conn, name)
                    for seq, idx_name, unique, origin, partial in idxs:
                        out.append(
                            f"-- index[{seq}]: {idx_name} "
                            f"{'(UNIQUE) ' if unique else ''}"
                            f"origin={origin} "
                            f"{'partial=1' if partial else 'partial=0'}"
                        )
                except Exception:
                    pass

            out.append(_ensure_semicolon(sql))
            out.append("")
        out.append("")

    emit_section("TABLES", tables)
    emit_section("INDEXES", indexes)
    emit_section("TRIGGERS", triggers)
    emit_section("VIEWS", views)

    out.append("COMMIT;")
    out.append("PRAGMA foreign_keys=ON;")
    out.append("")
    return out


def dump_schema(
    *,
    db_path: str,
    out_path: str,
    ensure_schema: bool = True,
    include_pragmas_as_comments: bool = False,
) -> str:
    if ensure_schema:
        init_db_schema()

    conn = sqlite3.connect(db_path)
    try:
        lines = _format_schema_sql(
            conn,
            db_path=db_path,
            out_path=out_path,
            include_pragmas_as_comments=include_pragmas_as_comments,
        )
    finally:
        conn.close()

    os.makedirs(os.path.dirname(os.path.abspath(out_path)) or ".", exist_ok=True)
    with open(out_path, "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(lines))
    return out_path


def main(argv: Optional[List[str]] = None) -> int:
    this_dir = Path(__file__).resolve().parent  # .../app/xraymgr
    default_out = str(this_dir / "current_schema.sql")

    p = argparse.ArgumentParser(description="Dump SQLite schema (DDL only) to a file.")
    p.add_argument(
        "--db",
        default="",
        help="SQLite DB path.\nDefault: project settings.get_db_path().",
    )
    p.add_argument(
        "--out",
        default="",
        help=f"Output file path.\nDefault: {default_out}",
    )
    p.add_argument(
        "--no-ensure",
        action="store_true",
        help="Do not run init_db_schema() before dumping.",
    )
    p.add_argument(
        "--with-pragmas",
        action="store_true",
        help="Include PRAGMA table/index info as SQL comments.",
    )

    args = p.parse_args(argv)
    db_path = args.db.strip() or get_db_path()
    out_path = args.out.strip() or default_out

    dump_schema(
        db_path=db_path,
        out_path=out_path,
        ensure_schema=not bool(args.no_ensure),
        include_pragmas_as_comments=bool(args.with_pragmas),
    )

    print(out_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
