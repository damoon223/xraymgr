#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import os
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

_THIS = Path(__file__).resolve()
_PKG = _THIS.parent
_APP = _PKG.parent
if str(_APP) not in sys.path:
    sys.path.insert(0, str(_APP))

try:
    from xraymgr.xray_runtime import XrayRuntimeApplier  # type: ignore
except Exception:
    XrayRuntimeApplier = None  # type: ignore

try:
    from xraymgr.test_settings import TEST_API_SERVER as _DEFAULT_API_SERVER  # type: ignore
except Exception:
    _DEFAULT_API_SERVER = "127.0.0.1:10085"


_STOP = threading.Event()
_STOP_REASON = ""


def _set_stop(reason: str) -> None:
    global _STOP_REASON
    if not _STOP.is_set():
        _STOP_REASON = (reason or "stop").strip()
    _STOP.set()


def _sig(signum: int, _frame) -> None:  # pragma: no cover
    _set_stop({signal.SIGTERM: "SIGTERM", signal.SIGINT: "SIGINT"}.get(signum, str(signum)))


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def sqlite_ts(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def oneline(s: str, n: int = 240) -> str:
    s = " ".join((s or "").replace("\r", " ").replace("\n", " ").replace("\t", " ").split())
    return s if len(s) <= n else s[: n - 3] + "..."


def oneword(s: str, n: int = 24) -> str:
    s = (s or "").strip()
    out = []
    for ch in s:
        if ch.isalnum() or ch in ("_", "-"):
            out.append(ch)
        else:
            break
    v = "".join(out) or "fail"
    return v[:n]


def stop_file_exists(p: str) -> bool:
    if not p:
        return False
    try:
        return Path(p).exists()
    except Exception:
        return False


def resolve_db_path(cli_db: str) -> str:
    p = (cli_db or "").strip()
    if p:
        return p
    env = (os.environ.get("XRAYMGR_DB_PATH") or "").strip()
    if env:
        return env
    if Path("/opt/xraymgr/data/xraymgr.db").exists():
        return "/opt/xraymgr/data/xraymgr.db"
    return str((_APP / "data" / "xraymgr.db").resolve())


def db_connect(db_path: str) -> sqlite3.Connection:
    c = sqlite3.connect(db_path, timeout=30, isolation_level=None)
    c.row_factory = sqlite3.Row
    try:
        c.execute("PRAGMA foreign_keys=ON;")
        c.execute("PRAGMA busy_timeout=30000;")
        c.execute("PRAGMA journal_mode=WAL;")
        c.execute("PRAGMA synchronous=NORMAL;")
    except Exception:
        pass
    return c


def table_exists(c: sqlite3.Connection, name: str) -> bool:
    return c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?;", (name,)).fetchone() is not None


def cols(c: sqlite3.Connection, table: str) -> List[str]:
    return [str(r[1]) for r in c.execute(f"PRAGMA table_info({table});").fetchall()]


def _ensure_columns(conn: sqlite3.Connection, table: str, wanted: Dict[str, str]) -> None:
    existing = set(cols(conn, table))
    cur = conn.cursor()
    for name, ddl in wanted.items():
        if name in existing:
            continue
        cur.execute(ddl)


def ensure_schema_minimal(conn: sqlite3.Connection) -> None:
    # لازم‌ها برای اینکه تست بچرخد و نتیجه‌ها ذخیره شود.
    if not table_exists(conn, "links") or not table_exists(conn, "inbound"):
        raise RuntimeError("required tables missing: links/inbound")

    _ensure_columns(
        conn,
        "links",
        {
            "is_config_primary": "ALTER TABLE links ADD COLUMN is_config_primary INTEGER",
            "config_json": "ALTER TABLE links ADD COLUMN config_json TEXT",
            "is_invalid": "ALTER TABLE links ADD COLUMN is_invalid INTEGER NOT NULL DEFAULT 0",
            "needs_replace": "ALTER TABLE links ADD COLUMN needs_replace INTEGER NOT NULL DEFAULT 0",
            "is_protocol_unsupported": "ALTER TABLE links ADD COLUMN is_protocol_unsupported INTEGER NOT NULL DEFAULT 0",
            "is_alive": "ALTER TABLE links ADD COLUMN is_alive INTEGER NOT NULL DEFAULT 0",
            "ip": "ALTER TABLE links ADD COLUMN ip TEXT",
            "country": "ALTER TABLE links ADD COLUMN country TEXT",
            "city": "ALTER TABLE links ADD COLUMN city TEXT",
            "datacenter": "ALTER TABLE links ADD COLUMN datacenter TEXT",
            "is_in_use": "ALTER TABLE links ADD COLUMN is_in_use INTEGER NOT NULL DEFAULT 0",
            "bound_port": "ALTER TABLE links ADD COLUMN bound_port INTEGER",
            "last_test_at": "ALTER TABLE links ADD COLUMN last_test_at DATETIME",
            "last_test_ok": "ALTER TABLE links ADD COLUMN last_test_ok INTEGER NOT NULL DEFAULT 0",
            "last_test_error": "ALTER TABLE links ADD COLUMN last_test_error TEXT",
            "test_status": "ALTER TABLE links ADD COLUMN test_status TEXT NOT NULL DEFAULT 'idle'",
            "test_started_at": "ALTER TABLE links ADD COLUMN test_started_at DATETIME",
            "test_lock_until": "ALTER TABLE links ADD COLUMN test_lock_until DATETIME",
            "test_lock_owner": "ALTER TABLE links ADD COLUMN test_lock_owner TEXT",
            "test_batch_id": "ALTER TABLE links ADD COLUMN test_batch_id TEXT",
            "inbound_tag": "ALTER TABLE links ADD COLUMN inbound_tag TEXT",
            "outbound_tag": "ALTER TABLE links ADD COLUMN outbound_tag TEXT",
        },
    )

    _ensure_columns(
        conn,
        "inbound",
        {
            "role": "ALTER TABLE inbound ADD COLUMN role TEXT NOT NULL DEFAULT 'test'",
            "is_active": "ALTER TABLE inbound ADD COLUMN is_active INTEGER NOT NULL DEFAULT 0",
            "status": "ALTER TABLE inbound ADD COLUMN status TEXT NOT NULL DEFAULT 'new'",
            "last_test_at": "ALTER TABLE inbound ADD COLUMN last_test_at DATETIME",
            "outbound_tag": "ALTER TABLE inbound ADD COLUMN outbound_tag TEXT",
            "link_id": "ALTER TABLE inbound ADD COLUMN link_id INTEGER REFERENCES links(id) ON DELETE RESTRICT",
        },
    )

    # ایندکس‌های حداقلی
    cur = conn.cursor()
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_links_test_status ON links(test_status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_links_test_lock_until ON links(test_lock_until)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_links_is_in_use ON links(is_in_use)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_inbound_role ON inbound(role)")
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_inbound_port_unique ON inbound(port)")
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_inbound_tag_unique ON inbound(tag)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_inbound_link_id ON inbound(link_id)")
    except Exception:
        pass


def _links_where_parts(links_cols: set[str], now_s: str) -> Tuple[str, List[Any]]:
    where = []
    params: List[Any] = []

    # primary
    if "is_config_primary" in links_cols:
        where.append("COALESCE(is_config_primary,0)=1")

    # json presence
    if "config_json" in links_cols:
        where += ["config_json IS NOT NULL", "TRIM(config_json)<>''"]

    # health
    if "is_invalid" in links_cols:
        where.append("COALESCE(is_invalid,0)=0")
    if "needs_replace" in links_cols:
        where.append("COALESCE(needs_replace,0)=0")
    if "is_protocol_unsupported" in links_cols:
        where.append("COALESCE(is_protocol_unsupported,0)=0")

    # in-use guard
    if "is_in_use" in links_cols:
        where.append("COALESCE(is_in_use,0)=0")

    # lock
    if "test_status" in links_cols and "test_lock_until" in links_cols:
        where.append(
            "(test_status='idle' OR test_status IS NULL OR "
            "(test_status='running' AND (test_lock_until IS NULL OR test_lock_until < ?)))"
        )
        params.append(now_s)
    elif "test_lock_until" in links_cols:
        where.append("(test_lock_until IS NULL OR test_lock_until < ?)")
        params.append(now_s)

    return " AND ".join(where) if where else "1=1", params


def count_eligible_links(conn: sqlite3.Connection, links_cols: set[str]) -> int:
    now_s = sqlite_ts(utc_now())
    where, params = _links_where_parts(links_cols, now_s)
    try:
        r = conn.execute(f"SELECT COUNT(*) AS c FROM links WHERE {where};", tuple(params)).fetchone()
        return int(r["c"]) if r else 0
    except Exception:
        return 0


def select_links(
    conn: sqlite3.Connection,
    links_cols: set[str],
    *,
    limit: int,
    batch_id: str,
    owner: str,
    lock_timeout: int,
) -> List[sqlite3.Row]:
    now = utc_now()
    now_s = sqlite_ts(now)
    lock_until = sqlite_ts(now + timedelta(seconds=int(lock_timeout)))

    where, params = _links_where_parts(links_cols, now_s)

    order = (
        "COALESCE(last_test_at,'1970-01-01 00:00:00') ASC, id ASC"
        if "last_test_at" in links_cols
        else "id ASC"
    )

    rows = conn.execute(
        f"SELECT * FROM links WHERE {where} ORDER BY {order} LIMIT ?",
        tuple(params + [int(limit)]),
    ).fetchall()

    # lock کردن انتخاب‌ها
    for r in rows:
        lid = int(r["id"])
        sets, args = [], []
        if "test_status" in links_cols:
            sets.append("test_status='running'")
        if "test_started_at" in links_cols:
            sets.append("test_started_at=?")
            args.append(now_s)
        if "test_lock_until" in links_cols:
            sets.append("test_lock_until=?")
            args.append(lock_until)
        if "test_lock_owner" in links_cols:
            sets.append("test_lock_owner=?")
            args.append(owner)
        if "test_batch_id" in links_cols:
            sets.append("test_batch_id=?")
            args.append(batch_id)
        if sets:
            args.append(lid)
            conn.execute(f"UPDATE links SET {', '.join(sets)} WHERE id=?", tuple(args))

    return rows


def ensure_test_inbounds(conn: sqlite3.Connection, ports: Sequence[int], prefix: str) -> None:
    cur = conn.cursor()
    for p in ports:
        tag = f"{prefix}{int(p)}"
        row = cur.execute("SELECT id FROM inbound WHERE role='test' AND port=? LIMIT 1", (int(p),)).fetchone()
        if row is None:
            cur.execute(
                "INSERT OR IGNORE INTO inbound(role,is_active,port,tag,link_id,outbound_tag,status,last_test_at) "
                "VALUES('test',0,?,?,NULL,NULL,'new',NULL)",
                (int(p), tag),
            )
        else:
            cur.execute("UPDATE inbound SET tag=? WHERE id=?", (tag, int(row["id"])))


def clear_test_inbounds(conn: sqlite3.Connection, ports: Sequence[int]) -> None:
    if not ports:
        return
    q = ",".join(["?"] * len(ports))
    conn.execute(
        f"UPDATE inbound SET link_id=NULL,outbound_tag=NULL,is_active=0,status='new' WHERE role='test' AND port IN ({q})",
        tuple(map(int, ports)),
    )


def fetch_inbounds(conn: sqlite3.Connection, ports: Sequence[int]) -> List[sqlite3.Row]:
    if not ports:
        return []
    q = ",".join(["?"] * len(ports))
    return list(
        conn.execute(
            f"SELECT * FROM inbound WHERE role='test' AND port IN ({q}) ORDER BY port",
            tuple(map(int, ports)),
        ).fetchall()
    )


def bind_inbound(conn: sqlite3.Connection, inbound_id: int, link_id: int, out_tag: str) -> None:
    conn.execute(
        "UPDATE inbound SET link_id=?,outbound_tag=?,is_active=1,status='running' WHERE id=?",
        (int(link_id), str(out_tag), int(inbound_id)),
    )


def release_inbound(conn: sqlite3.Connection, inbound_id: Optional[int]) -> None:
    if inbound_id:
        conn.execute(
            "UPDATE inbound SET link_id=NULL,outbound_tag=NULL,is_active=0,status='new',last_test_at=? WHERE id=?",
            (sqlite_ts(utc_now()), int(inbound_id)),
        )


def mark_link_bound(
    conn: sqlite3.Connection,
    links_cols: set[str],
    *,
    link_id: int,
    inbound_tag: str,
    outbound_tag: str,
    port: int,
) -> None:
    sets = []
    args: List[Any] = []
    if "is_in_use" in links_cols:
        sets.append("is_in_use=1")
    if "bound_port" in links_cols:
        sets.append("bound_port=?")
        args.append(int(port))
    if "inbound_tag" in links_cols:
        sets.append("inbound_tag=?")
        args.append(str(inbound_tag))
    if "outbound_tag" in links_cols:
        sets.append("outbound_tag=?")
        args.append(str(outbound_tag))
    if sets:
        args.append(int(link_id))
        conn.execute(f"UPDATE links SET {', '.join(sets)} WHERE id=?", tuple(args))


def unlock_link(conn: sqlite3.Connection, links_cols: set[str], link_id: int) -> None:
    sets = []
    if "test_status" in links_cols:
        sets.append("test_status='idle'")
    if "test_started_at" in links_cols:
        sets.append("test_started_at=NULL")
    if "test_lock_until" in links_cols:
        sets.append("test_lock_until=NULL")
    if "test_lock_owner" in links_cols:
        sets.append("test_lock_owner=NULL")
    if "test_batch_id" in links_cols:
        sets.append("test_batch_id=NULL")
    if "is_in_use" in links_cols:
        sets.append("is_in_use=0")
    if "bound_port" in links_cols:
        sets.append("bound_port=NULL")
    if "inbound_tag" in links_cols:
        sets.append("inbound_tag=NULL")
    if "outbound_tag" in links_cols:
        sets.append("outbound_tag=NULL")
    if sets:
        conn.execute(f"UPDATE links SET {', '.join(sets)} WHERE id=?", (int(link_id),))


def update_result(
    conn: sqlite3.Connection,
    links_cols: set[str],
    *,
    link_id: int,
    ok: bool,
    code: str,
    ip: Optional[str] = None,
    country: Optional[str] = None,
    city: Optional[str] = None,
    dc: Optional[str] = None,
    mark_proto_unsupported: bool = False,
) -> None:
    now_s = sqlite_ts(utc_now())
    sets = []
    args: List[Any] = []

    if "last_test_at" in links_cols:
        sets.append("last_test_at=?")
        args.append(now_s)
    if "last_test_ok" in links_cols:
        sets.append("last_test_ok=?")
        args.append(1 if ok else 0)
    if "last_test_error" in links_cols:
        sets.append("last_test_error=?")
        args.append(oneword(code))
    if "is_alive" in links_cols:
        sets.append("is_alive=?")
        args.append(1 if ok else 0)

    if (not ok) and mark_proto_unsupported and ("is_protocol_unsupported" in links_cols):
        sets.append("is_protocol_unsupported=1")

    # فقط در حالت OK مقادیر ipinfo را آپدیت کن (در fail قبلی‌ها حفظ شوند)
    if ok:
        if "ip" in links_cols and ip is not None:
            sets.append("ip=?")
            args.append(ip)
        if "country" in links_cols and country is not None:
            sets.append("country=?")
            args.append(country)
        if "city" in links_cols and city is not None:
            sets.append("city=?")
            args.append(city)
        if "datacenter" in links_cols and dc is not None:
            sets.append("datacenter=?")
            args.append(dc)

    if sets:
        args.append(int(link_id))
        conn.execute(f"UPDATE links SET {', '.join(sets)} WHERE id=?", tuple(args))


def parse_outbound(config_json: str) -> Dict[str, Any]:
    obj = json.loads(config_json)
    if isinstance(obj, dict) and isinstance(obj.get("outbounds"), list) and obj["outbounds"]:
        ob = obj["outbounds"][0]
        if not isinstance(ob, dict):
            raise ValueError("outbounds[0] not dict")
        return ob
    if isinstance(obj, dict) and ("protocol" in obj or "settings" in obj):
        return obj
    if isinstance(obj, list) and len(obj) == 1 and isinstance(obj[0], dict):
        return obj[0]
    raise ValueError("unexpected config shape")


def sanitize_outbound(ob: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(ob)
    out.pop("tag", None)
    out.pop("sendThrough", None)
    out.pop("mux", None)
    out.pop("proxySettings", None)
    return out


def socks_inbound(tag: str, listen: str, port: int, user: str, password: str) -> Dict[str, Any]:
    return {
        "tag": tag,
        "listen": listen,
        "port": int(port),
        "protocol": "socks",
        "settings": {"auth": "password", "accounts": [{"user": user, "pass": password}], "udp": True},
    }


def rule(rule_tag: str, inbound_tag: str, outbound_tag: str) -> Dict[str, Any]:
    return {"type": "field", "inboundTag": [inbound_tag], "outboundTag": outbound_tag, "tag": rule_tag}


def classify_prep_error(raw: str) -> Tuple[str, bool]:
    s = (raw or "").lower()
    if "unknown field" in s or "unknown protocol" in s or "unsupported protocol" in s:
        return "proto", True
    if "invalid json" in s or "json" in s:
        return "parse", False
    if "not found" in s:
        return "xray", False
    return "xray", False


def run_check(check_py: Path, *, socks5: str, timeout_sec: int) -> Dict[str, Any]:
    cmd = [sys.executable or "python3", "-u", str(check_py), "--timeout", str(int(timeout_sec)), "--socks5", socks5]
    try:
        p = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
    except Exception as e:
        return {"status": "error", "error_type": "spawn_failed", "error_detail": str(e)}

    t0 = time.time()
    while p.poll() is None:
        if _STOP.is_set():
            try:
                p.terminate()
            except Exception:
                pass
            break
        if time.time() - t0 >= float(timeout_sec) + 5.0:
            try:
                p.terminate()
            except Exception:
                pass
            break
        time.sleep(0.2)

    try:
        out, err = p.communicate(timeout=1.0)
    except Exception:
        out, err = "", ""

    if _STOP.is_set():
        return {"status": "error", "error_type": "stopped", "error_detail": _STOP_REASON or "stopped"}

    if (p.returncode or 0) != 0:
        try:
            js = json.loads(out) if out else {}
            if isinstance(js, dict):
                js.setdefault("status", "error")
                js.setdefault("error_type", "check_host_exit_nonzero")
                js.setdefault("error_detail", js.get("error_detail") or (err or oneline(out, 400)))
                return js
        except Exception:
            pass
        return {
            "status": "error",
            "error_type": "check_host_exit_nonzero",
            "error_detail": err or oneline(out, 400) or f"rc={p.returncode}",
        }

    try:
        js = json.loads(out)
        return js if isinstance(js, dict) else {"status": "error", "error_type": "badjson", "error_detail": "non-dict json"}
    except Exception:
        return {"status": "error", "error_type": "badjson", "error_detail": oneline(out, 400)}


def check_code(res: Dict[str, Any]) -> str:
    et = str(res.get("error_type") or "").strip()
    m = {
        "connection_timeout": "timeout",
        "connection_failed": "connect",
        "proxy_error": "proxy",
        "tls_error": "tls",
        "http_error": "http",
        "captcha_or_antibot_challenge": "antibot",
        "json_parse_failed": "parse",
        "badjson": "parse",
        "socks_missing_dependency": "socks",
        "spawn_failed": "spawn",
        "check_host_exit_nonzero": "fail",
        "unexpected_error": "fail",
    }
    return oneword(m.get(et, et or "fail"))


def extract_ip_fields(res: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    def norm(v: Any) -> Optional[str]:
        if v is None:
            return None
        s = str(v).strip()
        return s if s else None

    ip = norm(res.get("IP address"))
    country = norm(res.get("Country"))
    city = norm(res.get("City"))
    dc = norm(res.get("ISP"))

    if not ip:
        rh = res.get("resolved_host") or {}
        if isinstance(rh, dict):
            ip = norm(rh.get("host")) or ip
    return ip, country, city, dc


def log_ok(idx: int, link_id: int, ip: Optional[str], country: Optional[str], city: Optional[str], dc: Optional[str]) -> None:
    print(f"OK idx={idx} link={link_id} ip={ip or '-'} country={country or '-'} city={city or '-'} dc={dc or '-'}")
    sys.stdout.flush()


def log_fail(idx: int, link_id: int, reason: str) -> None:
    print(f"FAIL idx={idx} link={link_id} reason={oneword(reason)}")
    sys.stdout.flush()


def log_progress(eligible: int, tested: int, ok: int, fail: int) -> None:
    print(f"PROGRESS eligible={eligible} tested={tested} ok={ok} fail={fail}")
    sys.stdout.flush()


def run_batch(
    *,
    db_path: str,
    count: int,
    parallel: int,
    port_start: int,
    tag_prefix: str,
    lock_timeout: int,
    check_timeout: int,
    socks_user: str,
    socks_pass: str,
    socks_listen: str,
    xray_bin: str,
    api_server: str,
    owner: str,
    batch_id: str,
    stop_file: str,
) -> Tuple[bool, Dict[str, Any]]:
    if _STOP.is_set() or stop_file_exists(stop_file):
        _set_stop(_STOP_REASON or "stop")
        return False, {"status": "stopped"}

    if XrayRuntimeApplier is None:
        raise RuntimeError("XrayRuntimeApplier import failed")

    check_py = (_PKG / "check-host.py").resolve()
    if not check_py.exists():
        raise RuntimeError(f"check-host.py not found: {check_py}")

    ports = list(range(int(port_start), int(port_start) + int(count)))

    with db_connect(db_path) as c:
        ensure_schema_minimal(c)
        lcols = set(cols(c, "links"))
        eligible_total = count_eligible_links(c, lcols)

        c.execute("BEGIN IMMEDIATE")
        try:
            ensure_test_inbounds(c, ports, tag_prefix)
            clear_test_inbounds(c, ports)
            inbounds = fetch_inbounds(c, ports)

            links = select_links(c, lcols, limit=count, batch_id=batch_id, owner=owner, lock_timeout=lock_timeout)

            n = min(len(inbounds), len(links))
            inbounds, links = inbounds[:n], links[:n]
            c.commit()
        except Exception:
            c.rollback()
            raise

    if not inbounds or not links:
        return False, {"status": "idle", "eligible": eligible_total, "tested": 0, "ok": 0, "fail": 0}

    applier = XrayRuntimeApplier(
        xray_bin=xray_bin,
        api_server=api_server,
        exist_retry=True,
        command_timeout_sec=20.0,
        api_probe_timeout_sec=3.0,
    )

    created_out: List[str] = []
    created_in: List[str] = []
    created_rules: List[str] = []

    prog = {"eligible": int(eligible_total), "tested": 0, "ok": 0, "fail": 0}
    prog_lock = threading.Lock()

    def reporter() -> None:
        last = 0.0
        while True:
            if _STOP.is_set() or stop_file_exists(stop_file):
                return
            time.sleep(1.0)
            t = time.time()
            if t - last >= 10.0:
                last = t
                with prog_lock:
                    log_progress(prog["eligible"], prog["tested"], prog["ok"], prog["fail"])

    threading.Thread(target=reporter, daemon=True).start()

    jobs: List[Tuple[int, int, int, str, int, str, str]] = []

    with db_connect(db_path) as u:
        ensure_schema_minimal(u)
        lcols = set(cols(u, "links"))

        for idx, (inb, lnk) in enumerate(zip(inbounds, links), start=1):
            if _STOP.is_set() or stop_file_exists(stop_file):
                _set_stop(_STOP_REASON or "stop")
                break

            link_id = int(lnk["id"])
            inbound_id = int(inb["id"])
            port = int(inb["port"])
            inbound_tag = str(inb["tag"])

            out_tag = f"xT_{uuid.uuid4().hex[:10]}"
            rule_tag = f"rT_{uuid.uuid4().hex[:10]}"

            try:
                ob = sanitize_outbound(parse_outbound(str(lnk["config_json"] or "")))
                ob["tag"] = out_tag
            except Exception:
                u.execute("BEGIN IMMEDIATE")
                try:
                    update_result(u, lcols, link_id=link_id, ok=False, code="parse")
                    release_inbound(u, inbound_id)
                    unlock_link(u, lcols, link_id)
                    u.commit()
                except Exception:
                    u.rollback()
                    raise
                log_fail(idx, link_id, "parse")
                continue

            r1 = applier.add_outbound(ob)
            if not r1.get("ok"):
                raw = str(r1.get("stderr") or r1.get("stdout") or "xray_add_outbound_failed")
                code, mark = classify_prep_error(raw)
                u.execute("BEGIN IMMEDIATE")
                try:
                    update_result(u, lcols, link_id=link_id, ok=False, code=code, mark_proto_unsupported=mark)
                    release_inbound(u, inbound_id)
                    unlock_link(u, lcols, link_id)
                    u.commit()
                except Exception:
                    u.rollback()
                    raise
                log_fail(idx, link_id, code)
                continue
            created_out.append(out_tag)

            r2 = applier.add_inbound(socks_inbound(inbound_tag, socks_listen, port, socks_user, socks_pass))
            if not r2.get("ok"):
                try:
                    applier.remove_outbound(out_tag, ignore_not_found=True)
                except Exception:
                    pass
                if out_tag in created_out:
                    created_out.remove(out_tag)

                u.execute("BEGIN IMMEDIATE")
                try:
                    update_result(u, lcols, link_id=link_id, ok=False, code="xray")
                    release_inbound(u, inbound_id)
                    unlock_link(u, lcols, link_id)
                    u.commit()
                except Exception:
                    u.rollback()
                    raise
                log_fail(idx, link_id, "xray")
                continue
            created_in.append(inbound_tag)

            rr = applier.apply_rules({"routing": {"rules": [rule(rule_tag, inbound_tag, out_tag)]}}, append=True)
            if not rr.get("ok"):
                try:
                    applier.remove_inbound(inbound_tag, ignore_not_found=True)
                except Exception:
                    pass
                try:
                    applier.remove_outbound(out_tag, ignore_not_found=True)
                except Exception:
                    pass
                if inbound_tag in created_in:
                    created_in.remove(inbound_tag)
                if out_tag in created_out:
                    created_out.remove(out_tag)

                u.execute("BEGIN IMMEDIATE")
                try:
                    update_result(u, lcols, link_id=link_id, ok=False, code="rule")
                    release_inbound(u, inbound_id)
                    unlock_link(u, lcols, link_id)
                    u.commit()
                except Exception:
                    u.rollback()
                    raise
                log_fail(idx, link_id, "rule")
                continue
            created_rules.append(rule_tag)

            u.execute("BEGIN IMMEDIATE")
            try:
                bind_inbound(u, inbound_id, link_id, out_tag)
                mark_link_bound(u, lcols, link_id=link_id, inbound_tag=inbound_tag, outbound_tag=out_tag, port=port)
                u.commit()
            except Exception:
                u.rollback()
                raise

            jobs.append((idx, link_id, inbound_id, inbound_tag, port, out_tag, rule_tag))

    if not jobs:
        return True, {"status": "ok", "eligible": eligible_total, "tested": 0, "ok": 0, "fail": 0}

    def do_one(j: Tuple[int, int, int, str, int, str, str]) -> Dict[str, Any]:
        idx, link_id, inbound_id, inbound_tag, port, out_tag, rule_tag = j
        if _STOP.is_set() or stop_file_exists(stop_file):
            return {"idx": idx, "link_id": link_id, "inbound_id": inbound_id, "skipped": True, "why": _STOP_REASON or "stop"}

        socks5 = f"socks5h://{socks_user}:{socks_pass}@127.0.0.1:{port}"
        res = run_check(check_py, socks5=socks5, timeout_sec=check_timeout)

        if str(res.get("error_type") or "").strip().lower() in ("stopped", "stop"):
            return {"idx": idx, "link_id": link_id, "inbound_id": inbound_id, "skipped": True, "why": _STOP_REASON or "stop"}

        if str(res.get("status") or "").lower() == "ok":
            ip, country, city, dc = extract_ip_fields(res)
            return {"idx": idx, "link_id": link_id, "inbound_id": inbound_id, "ok": True, "ip": ip, "country": country, "city": city, "dc": dc}

        return {"idx": idx, "link_id": link_id, "inbound_id": inbound_id, "ok": False, "reason": check_code(res)}

    with ThreadPoolExecutor(max_workers=int(parallel)) as ex:
        futs = [ex.submit(do_one, j) for j in jobs]
        for fut in as_completed(futs):
            r = fut.result()
            link_id = int(r["link_id"])
            inbound_id = int(r["inbound_id"])
            idx = int(r["idx"])

            # STOP => fail محسوب نشود؛ فقط آزادسازی
            if r.get("skipped"):
                with db_connect(db_path) as u:
                    ensure_schema_minimal(u)
                    lcols = set(cols(u, "links"))
                    u.execute("BEGIN IMMEDIATE")
                    try:
                        release_inbound(u, inbound_id)
                        unlock_link(u, lcols, link_id)
                        u.commit()
                    except Exception:
                        u.rollback()
                        raise
                continue

            ok = bool(r.get("ok", False))

            with db_connect(db_path) as u:
                ensure_schema_minimal(u)
                lcols = set(cols(u, "links"))
                u.execute("BEGIN IMMEDIATE")
                try:
                    if ok:
                        update_result(
                            u,
                            lcols,
                            link_id=link_id,
                            ok=True,
                            code="ok",
                            ip=r.get("ip"),
                            country=r.get("country"),
                            city=r.get("city"),
                            dc=r.get("dc"),
                        )
                    else:
                        update_result(u, lcols, link_id=link_id, ok=False, code=str(r.get("reason") or "fail"))

                    release_inbound(u, inbound_id)
                    unlock_link(u, lcols, link_id)
                    u.commit()
                except Exception:
                    u.rollback()
                    raise

            with prog_lock:
                prog["tested"] += 1
                if ok:
                    prog["ok"] += 1
                else:
                    prog["fail"] += 1

            if ok:
                log_ok(idx, link_id, r.get("ip"), r.get("country"), r.get("city"), r.get("dc"))
            else:
                log_fail(idx, link_id, str(r.get("reason") or "fail"))

    # cleanup runtime resources
    try:
        if created_rules:
            applier.remove_rules(created_rules, ignore_not_found=True)
    except Exception:
        pass
    for t in list(created_in):
        try:
            applier.remove_inbound(t, ignore_not_found=True)
        except Exception:
            pass
    for t in list(created_out):
        try:
            applier.remove_outbound(t, ignore_not_found=True)
        except Exception:
            pass

    with prog_lock:
        rep = {"status": "ok", "eligible": prog["eligible"], "tested": prog["tested"], "ok": prog["ok"], "fail": prog["fail"]}
    print(f"SUMMARY tested={rep['tested']} ok={rep['ok']} fail={rep['fail']}")
    sys.stdout.flush()
    return True, rep


def main(argv: Optional[Sequence[str]] = None) -> int:
    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT, _sig)

    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="")
    ap.add_argument("--count", type=int, default=100)
    ap.add_argument("--parallel", type=int, default=10)
    ap.add_argument("--port-start", type=int, default=9000)
    ap.add_argument("--inbound-tag-prefix", default="in_test_")
    ap.add_argument("--lock-timeout", type=int, default=90)
    ap.add_argument("--check-timeout", type=int, default=60)
    ap.add_argument("--socks-user", default="me")
    ap.add_argument("--socks-pass", default="1")
    ap.add_argument("--socks-listen", default="127.0.0.1")
    ap.add_argument("--xray-bin", default="/usr/local/x-ui/bin/xray-linux-amd64")
    ap.add_argument("--api-server", default=str(_DEFAULT_API_SERVER))
    ap.add_argument("--owner", default="panel")
    ap.add_argument("--run-id", default="")
    ap.add_argument("--stop-file", default="")
    ap.add_argument("--idle-sleep", type=float, default=2.0)
    ap.add_argument("--max-batches", type=int, default=0)
    ap.add_argument("--continuous", action="store_true")
    ap.add_argument("--once", action="store_true")
    # backward-compat: این گزینه نگه داشته می‌شود ولی دیگر هیچ فایلی نوشته نمی‌شود
    ap.add_argument("--report-file", default="")  # ignored
    ap.add_argument("--data-dir", default="")  # ignored
    a = ap.parse_args(argv)

    # default: continuous
    continuous = bool(a.continuous) or (not bool(a.once))
    db_path = resolve_db_path(a.db)
    count = max(1, int(a.count or 100))
    parallel = max(1, int(a.parallel or 10))

    run_id = (str(a.run_id).strip() or uuid.uuid4().hex)

    stop_file = (str(a.stop_file).strip() or "")
    if stop_file and not os.path.isabs(stop_file):
        stop_file = str(Path(stop_file).expanduser().resolve())

    with db_connect(db_path) as c:
        ensure_schema_minimal(c)

    print(f"START mode={'continuous' if continuous else 'once'} count={count} parallel={parallel} db={db_path} api_server={a.api_server}")
    sys.stdout.flush()

    batches, total_ok, total_fail, total_tested = 0, 0, 0, 0
    t0 = utc_now()

    while True:
        if _STOP.is_set() or stop_file_exists(stop_file):
            _set_stop(_STOP_REASON or "stop")
            break
        if continuous and a.max_batches and batches >= int(a.max_batches):
            break

        batches += 1
        batch_id = f"{run_id}-{batches:06d}" if continuous else run_id

        had, rep = run_batch(
            db_path=db_path,
            count=count,
            parallel=parallel,
            port_start=int(a.port_start or 9000),
            tag_prefix=str(a.inbound_tag_prefix or "in_test_"),
            lock_timeout=int(a.lock_timeout or 90),
            check_timeout=int(a.check_timeout or 60),
            socks_user=str(a.socks_user or "me"),
            socks_pass=str(a.socks_pass or "1"),
            socks_listen=str(a.socks_listen or "127.0.0.1"),
            xray_bin=str(a.xray_bin),
            api_server=str(a.api_server),
            owner=str(a.owner or "panel"),
            batch_id=batch_id,
            stop_file=stop_file,
        )

        total_ok += int(rep.get("ok", 0) or 0)
        total_fail += int(rep.get("fail", 0) or 0)
        total_tested += int(rep.get("tested", 0) or 0)

        if not continuous:
            break

        if not had:
            time.sleep(max(0.2, float(a.idle_sleep)))
            continue

        time.sleep(0.1)

    dur = (utc_now() - t0).total_seconds()
    print(f"GLOBAL batches={batches} tested={total_tested} ok={total_ok} fail={total_fail} duration={dur:.2f}s stop={_STOP_REASON}")
    print("DONE")
    sys.stdout.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
