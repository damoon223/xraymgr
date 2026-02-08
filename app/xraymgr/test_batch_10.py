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
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

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


def nowlog() -> str:
    return utc_now().isoformat().replace("+00:00", "Z").replace("T", " ").replace("Z", " UTC")


def oneline(s: str, n: int = 240) -> str:
    s = " ".join((s or "").replace("\r", " ").replace("\n", " ").replace("\t", " ").split())
    return s if len(s) <= n else s[: n - 3] + "..."


def oneword(s: str, n: int = 32) -> str:
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
    # لازم‌ها برای اینکه تست هم بچرخد، هم نتیجه‌ها نوشته شود.
    if not table_exists(conn, "links") or not table_exists(conn, "inbound"):
        raise RuntimeError("required tables missing: links/inbound")

    _ensure_columns(
        conn,
        "links",
        {
            "is_config_primary": "ALTER TABLE links ADD COLUMN is_config_primary INTEGER",
            "is_invalid": "ALTER TABLE links ADD COLUMN is_invalid INTEGER NOT NULL DEFAULT 0",
            "is_protocol_unsupported": "ALTER TABLE links ADD COLUMN is_protocol_unsupported INTEGER NOT NULL DEFAULT 0",
            "config_json": "ALTER TABLE links ADD COLUMN config_json TEXT",
            "config_hash": "ALTER TABLE links ADD COLUMN config_hash VARCHAR(64)",
            "ip": "ALTER TABLE links ADD COLUMN ip TEXT",
            "country": "ALTER TABLE links ADD COLUMN country TEXT",
            "city": "ALTER TABLE links ADD COLUMN city TEXT",
            "datacenter": "ALTER TABLE links ADD COLUMN datacenter TEXT",
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
            "status": "ALTER TABLE inbound ADD COLUMN status TEXT NOT NULL DEFAULT 'new'",
            "last_test_at": "ALTER TABLE inbound ADD COLUMN last_test_at DATETIME",
            "outbound_tag": "ALTER TABLE inbound ADD COLUMN outbound_tag TEXT",
            "link_id": "ALTER TABLE inbound ADD COLUMN link_id INTEGER REFERENCES links(id) ON DELETE RESTRICT",
        },
    )

    # ایندکس‌های حداقلی برای performance (اگر نبودند)
    cur = conn.cursor()
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_links_test_status ON links(test_status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_links_test_lock_until ON links(test_lock_until)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_links_config_hash ON links(config_hash)")
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_inbound_port_unique ON inbound(port)")
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_inbound_tag_unique ON inbound(tag)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_inbound_role ON inbound(role)")
    except Exception:
        pass


def is_link_primary(conn: sqlite3.Connection, link_id: int) -> bool:
    row = conn.execute(
        "SELECT COALESCE(is_config_primary,0) AS p FROM links WHERE id=? LIMIT 1", (int(link_id),)
    ).fetchone()
    try:
        return bool(int(row["p"])) if row else False
    except Exception:
        return False


def ensure_test_inbounds(conn: sqlite3.Connection, ports: Sequence[int], prefix: str) -> None:
    for p in ports:
        tag = f"{prefix}{int(p)}"
        conn.execute(
            "INSERT OR IGNORE INTO inbound(role,is_active,port,tag,link_id,outbound_tag,status,last_test_at) "
            "VALUES('test',0,?,?,NULL,NULL,'new',NULL)",
            (int(p), tag),
        )


def clear_test_inbounds(conn: sqlite3.Connection, ports: Sequence[int]) -> None:
    if not ports:
        return
    q = ",".join(["?"] * len(ports))
    conn.execute(
        f"UPDATE inbound SET link_id=NULL,outbound_tag=NULL,is_active=0,status='new' WHERE role='test' AND port IN ({q})",
        [int(p) for p in ports],
    )


def fetch_slots(conn: sqlite3.Connection, ports: Sequence[int]) -> List[sqlite3.Row]:
    if not ports:
        return []
    q = ",".join(["?"] * len(ports))
    return conn.execute(
        f"SELECT id,port,tag FROM inbound WHERE role='test' AND port IN ({q}) ORDER BY port ASC",
        [int(p) for p in ports],
    ).fetchall()


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

    where = [
        "COALESCE(is_config_primary,0)=1",
        "config_json IS NOT NULL",
        "TRIM(config_json)<>''",
        "COALESCE(is_invalid,0)=0",
        "COALESCE(is_protocol_unsupported,0)=0",
    ]
    params: List[Any] = []

    # اگر lock/state داریم، از re-pick شدن همزمان جلوگیری کن
    if "test_status" in links_cols and "test_lock_until" in links_cols:
        where.append(
            "(test_status='idle' OR test_status IS NULL OR "
            "(test_status='running' AND (test_lock_until IS NULL OR test_lock_until < ?)))"
        )
        params.append(now_s)

    order = (
        "COALESCE(last_test_at,'1970-01-01 00:00:00') ASC, id ASC"
        if "last_test_at" in links_cols
        else "id ASC"
    )

    rows = conn.execute(
        f"SELECT * FROM links WHERE {' AND '.join(where)} ORDER BY {order} LIMIT ?",
        tuple(params + [int(limit)]),
    ).fetchall()

    # lock کردن انتخاب‌ها (اگر ستون‌ها موجودند)
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


def bind_slot(conn: sqlite3.Connection, links_cols: set[str], inbound_id: int, inbound_tag: str, link_id: int, out_tag: str) -> None:
    conn.execute(
        "UPDATE inbound SET link_id=?,outbound_tag=?,is_active=1,status='running' WHERE id=?",
        (int(link_id), str(out_tag), int(inbound_id)),
    )
    if "inbound_tag" in links_cols:
        conn.execute("UPDATE links SET inbound_tag=? WHERE id=?", (str(inbound_tag), int(link_id)))


def release_slot(conn: sqlite3.Connection, links_cols: set[str], inbound_id: Optional[int], link_id: int) -> None:
    if inbound_id:
        conn.execute(
            "UPDATE inbound SET link_id=NULL,outbound_tag=NULL,is_active=0,status='new',last_test_at=? WHERE id=?",
            (sqlite_ts(utc_now()), int(inbound_id)),
        )
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
    if "inbound_tag" in links_cols:
        sets.append("inbound_tag=NULL")
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
    isp: Optional[str] = None,
    mark_proto_unsupported: bool = False,
) -> None:
    sets, args = [], []
    now_s = sqlite_ts(utc_now())

    if "last_test_at" in links_cols:
        sets.append("last_test_at=?"); args.append(now_s)
    if "last_test_ok" in links_cols:
        sets.append("last_test_ok=?"); args.append(1 if ok else 0)
    if "last_test_error" in links_cols:
        sets.append("last_test_error=?"); args.append(oneword(code))
    if "is_alive" in links_cols:
        sets.append("is_alive=?"); args.append(1 if ok else 0)

    if (not ok) and mark_proto_unsupported and ("is_protocol_unsupported" in links_cols):
        sets.append("is_protocol_unsupported=1")

    if ok:
        if "ip" in links_cols and ip is not None:
            sets.append("ip=?"); args.append(ip)
        if "country" in links_cols and country is not None:
            sets.append("country=?"); args.append(country)
        if "city" in links_cols and city is not None:
            sets.append("city=?"); args.append(city)
        if "datacenter" in links_cols and isp is not None:
            sets.append("datacenter=?"); args.append(isp)

    if not sets:
        return
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
    raise ValueError("unexpected config_json shape")


def sanitize_outbound(ob: Dict[str, Any]) -> Dict[str, Any]:
    out = json.loads(json.dumps(ob, ensure_ascii=False))
    try:
        fp = out.get("streamSettings", {}).get("tlsSettings", {}).get("fingerprint")
        if isinstance(fp, str) and fp.strip().lower() == "none":
            out["streamSettings"]["tlsSettings"].pop("fingerprint", None)
    except Exception:
        pass
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
    return {"type": "field", "ruleTag": rule_tag, "inboundTag": [inbound_tag], "outboundTag": outbound_tag}


def classify_prep_error(detail: str) -> Tuple[str, bool]:
    s = (detail or "").lower()
    if "unknown cipher method" in s:
        return "ss_cipher", True
    if "failed to build outbound handler" in s or "unknown protocol" in s:
        return "proto", True
    return "xray", False


def run_check(check_py: Path, *, socks5: str, timeout_sec: int) -> Dict[str, Any]:
    cmd = [sys.executable or "python3", str(check_py), "--timeout", str(int(timeout_sec)), "--socks5", socks5]
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
        if time.time() - t0 >= float(timeout_sec):
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
                js.setdefault("error_detail", js.get("error_detail") or (err or out))
                return js
        except Exception:
            pass
        return {"status": "error", "error_type": "check_host_exit_nonzero", "error_detail": err or out or f"rc={p.returncode}"}
    try:
        js = json.loads(out)
        return js if isinstance(js, dict) else {"status": "error", "error_type": "badjson", "error_detail": "non-dict json"}
    except Exception:
        return {"status": "error", "error_type": "badjson", "error_detail": oneline(out, 400)}


def check_code(res: Dict[str, Any]) -> Tuple[str, str]:
    et, ed = str(res.get("error_type") or "").strip(), str(res.get("error_detail") or "").strip()
    m = {
        "connection_timeout": "timeout",
        "connection_failed": "connect",
        "proxy_error": "proxy",
        "tls_error": "tls",
        "http_error": "http",
        "captcha_or_antibot_challenge": "antibot",
    }
    code = m.get(et, et or "fail")
    return oneword(code), oneline(f"{et}:{ed}".strip(":"), 240)


def extract_ip_country_isp(res: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    # primary keys from check-host.py
    ip = res.get("IP address")
    country = res.get("Country")
    city = res.get("City")
    isp = res.get("ISP")

    # fallback: اگر ip-info خالی بود، از resolved_host.host (که از /me استخراج می‌شود) استفاده کن
    if not ip:
        rh = res.get("resolved_host") or {}
        if isinstance(rh, dict):
            ip = rh.get("host") or ip

    def norm(v: Any) -> Optional[str]:
        if v is None:
            return None
        s = str(v).strip()
        return s if s else None

    return norm(ip), norm(country), norm(city), norm(isp)


def write_report(data_dir: Path, *, count: int, batch_id: str, report_file: str, report: Dict[str, Any]) -> Optional[Path]:
    rf = (report_file or "auto").strip()
    rp = data_dir / f"test_report_{count}_{batch_id}.json" if rf.lower() == "auto" else Path(rf).expanduser().resolve()
    try:
        rp.parent.mkdir(parents=True, exist_ok=True)
        rp.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return rp
    except Exception:
        return None


def run_batch(
    *,
    db_path: str,
    data_dir: Path,
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
    report_file: str,
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
        links_cols = set(cols(c, "links"))

        c.execute("BEGIN IMMEDIATE")
        try:
            ensure_test_inbounds(c, ports, tag_prefix)
            clear_test_inbounds(c, ports)
            slots = fetch_slots(c, ports)
            links = select_links(c, links_cols, limit=count, batch_id=batch_id, owner=owner, lock_timeout=lock_timeout)
            n = min(len(slots), len(links))
            slots, links = slots[:n], links[:n]
            c.commit()
        except Exception:
            c.rollback()
            raise

    if not slots or not links:
        return False, {"status": "idle", "batch_id": batch_id}

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
    alloc_pairs = [(int(s["id"]), int(l["id"])) for s, l in zip(slots, links)]
    released: set[int] = set()
    ok_items: List[Dict[str, Any]] = []
    fail_items: List[Dict[str, Any]] = []
    started = utc_now()

    def log_fail(phase: str, idx: int, link_id: int, code: str, detail: str) -> None:
        print(f"[{nowlog()}] FAIL({phase}) #{idx} link_id={link_id} code={oneword(code)} detail={oneline(detail)}")
        sys.stdout.flush()

    def log_ok(idx: int, link_id: int, ip: str, country: str, dc: str) -> None:
        print(f"[{nowlog()}] OK(check) #{idx} link_id={link_id} ip={ip or '-'} country={country or '-'} dc={dc or '-'}")
        sys.stdout.flush()

    with db_connect(db_path) as u:
        ensure_schema_minimal(u)
        lcols = set(cols(u, "links"))

        try:
            jobs: List[Tuple[int, int, int, str, int, str, str]] = []

            for idx, (s, l) in enumerate(zip(slots, links), start=1):
                if _STOP.is_set() or stop_file_exists(stop_file):
                    _set_stop(_STOP_REASON or "stop")
                    break

                link_id, inbound_id = int(l["id"]), int(s["id"])
                port, inbound_tag = int(s["port"]), str(s["tag"])
                out_tag, rule_tag = f"xT_{uuid.uuid4().hex[:10]}", f"rT_{uuid.uuid4().hex[:10]}"

                # چک دوباره primary بودن (برای جلوگیری از race)
                if not is_link_primary(u, link_id):
                    log_fail("prep", idx, link_id, "not_primary", "link is not primary at execution time")
                    u.execute("BEGIN IMMEDIATE")
                    try:
                        update_result(u, lcols, link_id=link_id, ok=False, code="not_primary")
                        release_slot(u, lcols, inbound_id, link_id)
                        u.commit()
                    except Exception:
                        u.rollback()
                        raise
                    released.add(link_id)
                    continue

                try:
                    ob = sanitize_outbound(parse_outbound(str(l["config_json"])))
                    ob["tag"] = out_tag
                except Exception as e:
                    log_fail("prep", idx, link_id, "parse", str(e))
                    u.execute("BEGIN IMMEDIATE")
                    try:
                        update_result(u, lcols, link_id=link_id, ok=False, code="parse")
                        release_slot(u, lcols, inbound_id, link_id)
                        u.commit()
                    except Exception:
                        u.rollback()
                        raise
                    released.add(link_id)
                    continue

                r1 = applier.add_outbound(ob)
                if not r1.get("ok"):
                    raw = str(r1.get("stderr") or r1.get("stdout") or "xray_add_outbound_failed")
                    code, mark = classify_prep_error(raw)
                    log_fail("prep", idx, link_id, code, raw)
                    u.execute("BEGIN IMMEDIATE")
                    try:
                        update_result(u, lcols, link_id=link_id, ok=False, code=code, mark_proto_unsupported=mark)
                        release_slot(u, lcols, inbound_id, link_id)
                        u.commit()
                    except Exception:
                        u.rollback()
                        raise
                    released.add(link_id)
                    continue
                created_out.append(out_tag)

                r2 = applier.add_inbound(socks_inbound(inbound_tag, socks_listen, port, socks_user, socks_pass))
                if not r2.get("ok"):
                    raw = str(r2.get("stderr") or r2.get("stdout") or "xray_add_inbound_failed")
                    try:
                        applier.remove_outbound(out_tag, ignore_not_found=True)
                    except Exception:
                        pass
                    if out_tag in created_out:
                        created_out.remove(out_tag)
                    log_fail("prep", idx, link_id, "xray", raw)
                    u.execute("BEGIN IMMEDIATE")
                    try:
                        update_result(u, lcols, link_id=link_id, ok=False, code="xray")
                        release_slot(u, lcols, inbound_id, link_id)
                        u.commit()
                    except Exception:
                        u.rollback()
                        raise
                    released.add(link_id)
                    continue
                created_in.append(inbound_tag)

                rr = applier.apply_rules({"routing": {"rules": [rule(rule_tag, inbound_tag, out_tag)]}}, append=True)
                if not rr.get("ok"):
                    raw = str(rr.get("stderr") or rr.get("stdout") or "xray_adrules_failed")
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
                    log_fail("prep", idx, link_id, "rule", raw)
                    u.execute("BEGIN IMMEDIATE")
                    try:
                        update_result(u, lcols, link_id=link_id, ok=False, code="rule")
                        release_slot(u, lcols, inbound_id, link_id)
                        u.commit()
                    except Exception:
                        u.rollback()
                        raise
                    released.add(link_id)
                    continue
                created_rules.append(rule_tag)

                u.execute("BEGIN IMMEDIATE")
                try:
                    bind_slot(u, lcols, inbound_id, inbound_tag, link_id, out_tag)
                    u.commit()
                except Exception:
                    u.rollback()
                    raise

                jobs.append((idx, link_id, inbound_id, inbound_tag, port, out_tag, rule_tag))

            if not jobs:
                rep = {
                    "status": "ok",
                    "batch_id": batch_id,
                    "db": db_path,
                    "count_requested": count,
                    "count_tested": 0,
                    "summary": {"ok": 0, "fail": 0, "tested": 0},
                    "ok": [],
                    "fail": [],
                }
                rp = write_report(data_dir, count=count, batch_id=batch_id, report_file=report_file, report=rep)
                if rp:
                    print(f"[{nowlog()}] REPORT_FILE {rp}")
                print(f"[{nowlog()}] SUMMARY batch_id={batch_id} tested=0 ok=0 fail=0 duration=0.00s")
                sys.stdout.flush()
                return True, rep

            def do_one(j: Tuple[int, int, int, str, int, str, str]) -> Dict[str, Any]:
                idx, link_id, inbound_id, inbound_tag, port, out_tag, rule_tag = j
                if _STOP.is_set() or stop_file_exists(stop_file):
                    return {
                        "idx": idx,
                        "link_id": link_id,
                        "inbound_id": inbound_id,
                        "port": port,
                        "ok": False,
                        "error": "stopped",
                        "error_detail": _STOP_REASON or "stopped",
                        "duration_sec": 0.0,
                    }
                t0 = time.time()
                socks5 = f"socks5h://{socks_user}:{socks_pass}@127.0.0.1:{port}"
                res = run_check(check_py, socks5=socks5, timeout_sec=check_timeout)
                dur = round(time.time() - t0, 3)

                if str(res.get("status") or "").lower() == "ok":
                    ip, country, city, isp = extract_ip_country_isp(res)
                    return {
                        "idx": idx,
                        "link_id": link_id,
                        "inbound_id": inbound_id,
                        "port": port,
                        "ok": True,
                        "error": "ok",
                        "error_detail": "",
                        "duration_sec": dur,
                        "ip": ip,
                        "country": country,
                        "city": city,
                        "isp": isp,
                    }

                code, detail = check_code(res)
                return {
                    "idx": idx,
                    "link_id": link_id,
                    "inbound_id": inbound_id,
                    "port": port,
                    "ok": False,
                    "error": code,
                    "error_detail": detail,
                    "duration_sec": dur,
                }

            with ThreadPoolExecutor(max_workers=int(parallel)) as ex:
                futs = [ex.submit(do_one, j) for j in jobs]
                for fut in as_completed(futs):
                    if _STOP.is_set() or stop_file_exists(stop_file):
                        _set_stop(_STOP_REASON or "stop")
                    r = fut.result()
                    link_id, inbound_id = int(r["link_id"]), int(r["inbound_id"])
                    ok = bool(r["ok"])

                    if ok:
                        ip = str(r.get("ip") or "").strip()
                        country = str(r.get("country") or "").strip()
                        dc = str(r.get("isp") or "").strip()

                        ok_items.append(
                            {
                                "idx": int(r["idx"]),
                                "link_id": link_id,
                                "port": int(r["port"]),
                                "ip": ip or None,
                                "country": country or None,
                                "city": (str(r.get("city") or "").strip() or None),
                                "datacenter": dc or None,
                                "duration_sec": r.get("duration_sec"),
                            }
                        )
                        log_ok(int(r["idx"]), link_id, ip, country, dc)
                    else:
                        fail_items.append(
                            {
                                "idx": int(r["idx"]),
                                "link_id": link_id,
                                "port": int(r["port"]),
                                "error": oneword(str(r["error"])),
                                "error_detail": oneline(str(r.get("error_detail") or "")),
                                "duration_sec": r.get("duration_sec"),
                            }
                        )
                        log_fail("check", int(r["idx"]), link_id, str(r["error"]), str(r.get("error_detail") or ""))

                    u.execute("BEGIN IMMEDIATE")
                    try:
                        if ok:
                            update_result(
                                u,
                                lcols,
                                link_id=link_id,
                                ok=True,
                                code="ok",
                                ip=(str(r.get("ip") or "").strip() or None),
                                country=(str(r.get("country") or "").strip() or None),
                                city=(str(r.get("city") or "").strip() or None),
                                isp=(str(r.get("isp") or "").strip() or None),
                            )
                        else:
                            update_result(u, lcols, link_id=link_id, ok=False, code=str(r["error"]))

                        release_slot(u, lcols, inbound_id, link_id)
                        u.commit()
                    except Exception:
                        u.rollback()
                        raise
                    released.add(link_id)

        finally:
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
            leftovers = [(inb, lid) for inb, lid in alloc_pairs if lid not in released]
            if leftovers:
                u.execute("BEGIN IMMEDIATE")
                try:
                    for inb, lid in leftovers:
                        release_slot(u, lcols, inb, lid)
                    u.commit()
                except Exception:
                    u.rollback()

    dur = round((utc_now() - started).total_seconds(), 3)
    rep = {
        "status": "ok",
        "batch_id": batch_id,
        "db": db_path,
        "count_requested": count,
        "count_tested": len(ok_items) + len(fail_items),
        "parallel": parallel,
        "summary": {"ok": len(ok_items), "fail": len(fail_items), "tested": len(ok_items) + len(fail_items)},
        "ok": ok_items,
        "fail": fail_items,
        "duration_sec": dur,
    }
    rp = write_report(data_dir, count=count, batch_id=batch_id, report_file=report_file, report=rep)
    if rp:
        print(f"[{nowlog()}] REPORT_FILE {rp}")
    print(f"[{nowlog()}] SUMMARY batch_id={batch_id} tested={len(ok_items)+len(fail_items)} ok={len(ok_items)} fail={len(fail_items)} duration={dur:.2f}s")
    sys.stdout.flush()
    return True, rep


def main(argv: Optional[List[str]] = None) -> int:
    try:
        signal.signal(signal.SIGTERM, _sig)
        signal.signal(signal.SIGINT, _sig)
    except Exception:
        pass

    ap = argparse.ArgumentParser(description="xraymgr batch tester (continuous by default)")
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
    ap.add_argument("--data-dir", default="auto")
    ap.add_argument("--report-file", default="auto")
    ap.add_argument("--stop-file", default="")
    ap.add_argument("--idle-sleep", type=float, default=2.0)
    ap.add_argument("--max-batches", type=int, default=0)
    ap.add_argument("--continuous", action="store_true")
    ap.add_argument("--once", action="store_true")
    a = ap.parse_args(argv)

    continuous = bool(a.continuous) or (not bool(a.once))
    db_path = resolve_db_path(a.db)
    count = max(1, int(a.count or 100))
    parallel = max(1, int(a.parallel or 10))
    run_id = (str(a.run_id).strip() or uuid.uuid4().hex)

    data_dir = Path(db_path).expanduser().resolve().parent if str(a.data_dir).strip().lower() == "auto" else Path(str(a.data_dir)).expanduser().resolve()
    data_dir.mkdir(parents=True, exist_ok=True)

    stop_file = (str(a.stop_file).strip() or "")
    if stop_file and not os.path.isabs(stop_file):
        stop_file = str(Path(stop_file).expanduser().resolve())

    # ensure schema on this DB (مهم برای ip/country/datacenter + lock columns)
    with db_connect(db_path) as c:
        ensure_schema_minimal(c)

    print(f"[{nowlog()}] start run_id={run_id} mode={'continuous' if continuous else 'once'} count={count} parallel={parallel} db={db_path} api_server={a.api_server}")
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
            data_dir=data_dir,
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
            report_file=str(a.report_file or "auto"),
            stop_file=stop_file,
        )

        try:
            sm = rep.get("summary") or {}
            total_ok += int(sm.get("ok", 0))
            total_fail += int(sm.get("fail", 0))
            total_tested += int(sm.get("tested", 0))
        except Exception:
            pass

        if not continuous:
            break

        if not had:
            time.sleep(max(0.2, float(a.idle_sleep)))
            continue

        time.sleep(0.1)

    dur = (utc_now() - t0).total_seconds()
    print(f"[{nowlog()}] GLOBAL_SUMMARY run_id={run_id} batches={batches} tested={total_tested} ok={total_ok} fail={total_fail} duration={dur:.2f}s stop_reason={_STOP_REASON}")
    print(f"[{nowlog()}] done")
    sys.stdout.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
