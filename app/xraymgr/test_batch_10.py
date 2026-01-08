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
    from xraymgr.settings import get_db_path as _get_db_path  # type: ignore
except Exception:
    _get_db_path = None  # type: ignore
_STOP = threading.Event()
_STOP_REASON = ""
def _set_stop(reason: str) -> None:
    global _STOP_REASON
    if not _STOP.is_set():
        _STOP_REASON = (reason or "stop").strip()
        _STOP.set()
def _sig_handler(signum: int, _frame) -> None:  # pragma: no cover
    name = {signal.SIGTERM: "SIGTERM", signal.SIGINT: "SIGINT"}.get(signum, str(signum))
    _set_stop(name)
def utc_now() -> datetime:
    return datetime.now(timezone.utc)
def sqlite_ts(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
def iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
def nowlog() -> str:
    return iso_utc(utc_now()).replace("T", " ").replace("Z", " UTC")
def one_line(s: str, max_len: int = 240) -> str:
    s = (s or "").replace("\r", " ").replace("\n", " ").replace("\t", " ").strip()
    s = " ".join(s.split())
    return s if len(s) <= max_len else (s[: max_len - 3] + "...")
def one_word(s: str, max_len: int = 32) -> str:
    s = (s or "").strip()
    if not s:
        return "fail"
    out = []
    for ch in s:
        if ch.isalnum() or ch in ("_", "-"):
            out.append(ch)
        else:
            break
    v = "".join(out) or "fail"
    return v[:max_len]
def stop_file_exists(path: str) -> bool:
    if not path:
        return False
    try:
        return Path(path).exists()
    except Exception:
        return False
def resolve_db_path(cli_db: str) -> str:
    p = (cli_db or "").strip()
    if p:
        return p
    env = (os.environ.get("XRAYMGR_DB_PATH") or "").strip()
    if env:
        return env
    if _get_db_path:
        try:
            g = str(_get_db_path()).strip()
            if g:
                return g
        except Exception:
            pass
    if Path("/opt/xraymgr/data/xraymgr.db").exists():
        return "/opt/xraymgr/data/xraymgr.db"
    return str((_APP / "data" / "xraymgr.db").resolve())
def db_connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute("PRAGMA busy_timeout=30000;")
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
    except Exception:
        pass
    return conn
def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    r = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (name,)).fetchone()
    return r is not None
def cols(conn: sqlite3.Connection, table: str) -> List[str]:
    return [str(r[1]) for r in conn.execute(f"PRAGMA table_info({table});").fetchall()]
def ensure_required(conn: sqlite3.Connection) -> None:
    missing = []
    for t in ("links", "inbound"):
        if not table_exists(conn, t):
            missing.append(t)
    if missing:
        raise RuntimeError(f"required table missing: {', '.join(missing)}")
def ensure_test_inbounds(conn: sqlite3.Connection, ports: Sequence[int], tag_prefix: str) -> None:
    for p in ports:
        tag = f"{tag_prefix}{int(p)}"
        conn.execute(
            """
            INSERT OR IGNORE INTO inbound
              (role, is_active, port, tag, link_id, outbound_tag, status, last_test_at)
            VALUES
              ('test', 0, ?, ?, NULL, NULL, 'new', NULL)
            """,
            (int(p), tag),
        )
def clear_test_inbounds(conn: sqlite3.Connection, ports: Sequence[int]) -> None:
    if not ports:
        return
    q = ",".join(["?"] * len(ports))
    conn.execute(
        f"""
        UPDATE inbound
        SET link_id=NULL, outbound_tag=NULL, is_active=0, status='new'
        WHERE role='test' AND port IN ({q})
        """,
        [int(p) for p in ports],
    )
def fetch_test_inbounds(conn: sqlite3.Connection, ports: Sequence[int]) -> List[sqlite3.Row]:
    if not ports:
        return []
    q = ",".join(["?"] * len(ports))
    return conn.execute(
        f"""
        SELECT id, port, tag
        FROM inbound
        WHERE role='test' AND port IN ({q})
        ORDER BY port ASC
        """,
        [int(p) for p in ports],
    ).fetchall()
def select_links(
    conn: sqlite3.Connection,
    cols_links: set[str],
    *,
    limit: int,
    batch_id: str,
    owner: str,
    lock_timeout_sec: int,
) -> List[sqlite3.Row]:
    now = utc_now()
    now_s = sqlite_ts(now)
    lock_until_s = sqlite_ts(now + timedelta(seconds=int(lock_timeout_sec)))
    where = [
        "COALESCE(is_config_primary,0)=1",
        "config_json IS NOT NULL",
        "TRIM(config_json) <> ''",
        "COALESCE(is_invalid,0)=0",
        "COALESCE(is_protocol_unsupported,0)=0",
    ]
    params: List[Any] = []
    if "test_status" in cols_links and "test_lock_until" in cols_links:
        where.append(
            "(test_status='idle' OR test_status IS NULL OR (test_status='running' AND (test_lock_until IS NULL OR test_lock_until < ?)))"
        )
        params.append(now_s)
    order = "COALESCE(last_test_at,'1970-01-01 00:00:00') ASC, id ASC" if "last_test_at" in cols_links else "id ASC"
    rows = conn.execute(
        f"SELECT * FROM links WHERE {' AND '.join(where)} ORDER BY {order} LIMIT ?;",
        tuple(params + [int(limit)]),
    ).fetchall()
    for r in rows:
        lid = int(r["id"])
        set_parts: List[str] = []
        args: List[Any] = []
        if "test_status" in cols_links:
            set_parts.append("test_status='running'")
        if "test_started_at" in cols_links:
            set_parts.append("test_started_at=?")
            args.append(now_s)
        if "test_lock_until" in cols_links:
            set_parts.append("test_lock_until=?")
            args.append(lock_until_s)
        if "test_lock_owner" in cols_links:
            set_parts.append("test_lock_owner=?")
            args.append(owner)
        if "test_batch_id" in cols_links:
            set_parts.append("test_batch_id=?")
            args.append(batch_id)
        if set_parts:
            args.append(lid)
            conn.execute(f"UPDATE links SET {', '.join(set_parts)} WHERE id=?;", tuple(args))
    return rows
def bind_slot(conn: sqlite3.Connection, cols_links: set[str], inbound_id: int, inbound_tag: str, link_id: int, out_tag: str) -> None:
    conn.execute(
        "UPDATE inbound SET link_id=?, outbound_tag=?, is_active=1, status='running' WHERE id=?;",
        (int(link_id), str(out_tag), int(inbound_id)),
    )
    if "inbound_tag" in cols_links:
        conn.execute("UPDATE links SET inbound_tag=? WHERE id=?;", (str(inbound_tag), int(link_id)))
def release_slot(conn: sqlite3.Connection, cols_links: set[str], inbound_id: Optional[int], link_id: int) -> None:
    if inbound_id:
        conn.execute(
            "UPDATE inbound SET link_id=NULL, outbound_tag=NULL, is_active=0, status='new', last_test_at=? WHERE id=?;",
            (sqlite_ts(utc_now()), int(inbound_id)),
        )
    set_parts: List[str] = []
    if "test_status" in cols_links:
        set_parts.append("test_status='idle'")
    if "test_started_at" in cols_links:
        set_parts.append("test_started_at=NULL")
    if "test_lock_until" in cols_links:
        set_parts.append("test_lock_until=NULL")
    if "test_lock_owner" in cols_links:
        set_parts.append("test_lock_owner=NULL")
    if "test_batch_id" in cols_links:
        set_parts.append("test_batch_id=NULL")
    if "inbound_tag" in cols_links:
        set_parts.append("inbound_tag=NULL")
    if set_parts:
        conn.execute(f"UPDATE links SET {', '.join(set_parts)} WHERE id=?;", (int(link_id),))
def update_result(
    conn: sqlite3.Connection,
    cols_links: set[str],
    *,
    link_id: int,
    ok: bool,
    error_code: str,
    ip: Optional[str] = None,
    country: Optional[str] = None,
    city: Optional[str] = None,
    isp: Optional[str] = None,
    mark_proto_unsupported: bool = False,
) -> None:
    now_s = sqlite_ts(utc_now())
    sets = ["last_test_at=?"]
    args: List[Any] = [now_s]
    if "last_test_ok" in cols_links:
        sets.append("last_test_ok=?")
        args.append(1 if ok else 0)
    if "last_test_error" in cols_links:
        sets.append("last_test_error=?")
        args.append(one_word(error_code))
    if "is_alive" in cols_links:
        sets.append("is_alive=?")
        args.append(1 if ok else 0)
    if (not ok) and mark_proto_unsupported and "is_protocol_unsupported" in cols_links:
        sets.append("is_protocol_unsupported=1")
    if ok:
        if "ip" in cols_links and ip:
            sets.append("ip=?")
            args.append(ip)
        if "country" in cols_links and country:
            sets.append("country=?")
            args.append(country)
        if "city" in cols_links and city:
            sets.append("city=?")
            args.append(city)
        if "datacenter" in cols_links and isp:
            sets.append("datacenter=?")
            args.append(isp)
    args.append(int(link_id))
    conn.execute(f"UPDATE links SET {', '.join(sets)} WHERE id=?;", tuple(args))
def count_is_alive_1(db_path: str) -> Optional[int]:
    try:
        conn = db_connect(db_path)
        try:
            r = conn.execute("SELECT COUNT(*) FROM links WHERE is_alive=1;").fetchone()
            return int(r[0]) if r else 0
        finally:
            conn.close()
    except Exception:
        return None
def parse_outbound(config_json: str) -> Dict[str, Any]:
    obj = json.loads(config_json)
    if isinstance(obj, dict) and isinstance(obj.get("outbounds"), list) and obj["outbounds"]:
        first = obj["outbounds"][0]
        if not isinstance(first, dict):
            raise ValueError("outbounds[0] not dict")
        return first
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
def check_host(check_host_py: Path, *, socks5_url: str, timeout_sec: int) -> Dict[str, Any]:
    py = sys.executable or "python3"
    cmd = [py, str(check_host_py), "--timeout", str(int(timeout_sec)), "--socks5", str(socks5_url)]
    try:
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding="utf-8", errors="replace")
    except Exception as e:
        return {"status": "error", "error_type": "spawn_failed", "error_detail": str(e)}
    t0 = time.time()
    while True:
        if _STOP.is_set():
            try:
                p.terminate()
            except Exception:
                pass
            break
        if (time.time() - t0) >= float(timeout_sec):
            try:
                p.terminate()
            except Exception:
                pass
            break
        rc = p.poll()
        if rc is not None:
            break
        time.sleep(0.2)
    try:
        out, err = p.communicate(timeout=1.0)
    except Exception:
        out, err = "", ""
    if _STOP.is_set():
        return {"status": "error", "error_type": "stopped", "error_detail": _STOP_REASON or "stopped"}
    rc = p.returncode if p.returncode is not None else -1
    if rc != 0:
        try:
            js = json.loads(out) if out else {}
            if isinstance(js, dict):
                js.setdefault("status", "error")
                js.setdefault("error_type", "check_host_exit_nonzero")
                js.setdefault("error_detail", js.get("error_detail") or (err or out))
                return js
        except Exception:
            pass
        return {"status": "error", "error_type": "check_host_exit_nonzero", "error_detail": err or out or f"rc={rc}"}
    try:
        js = json.loads(out)
        if isinstance(js, dict):
            return js
        return {"status": "error", "error_type": "badjson", "error_detail": "non-dict json"}
    except Exception:
        return {"status": "error", "error_type": "badjson", "error_detail": one_line(out, 400)}
def classify_prep_error(detail: str) -> Tuple[str, bool]:
    s = (detail or "").lower()
    if "unknown cipher method" in s:
        return "ss_cipher", True
    if "failed to build outbound handler" in s or "unknown protocol" in s:
        return "proto", True
    return "xray", False
def check_error_code(res: Dict[str, Any]) -> Tuple[str, str]:
    et = str(res.get("error_type") or "").strip()
    ed = str(res.get("error_detail") or "").strip()
    if et == "connection_timeout":
        code = "timeout"
    elif et == "connection_failed":
        code = "connect"
    elif et == "proxy_error":
        code = "proxy"
    elif et == "tls_error":
        code = "tls"
    elif et == "http_error":
        code = "http"
    elif et == "captcha_or_antibot_challenge":
        code = "antibot"
    elif et:
        code = et
    else:
        code = "fail"
    detail = one_line(f"{et}:{ed}".strip(":"), 240)
    return one_word(code), detail
def socks_inbound(*, tag: str, listen: str, port: int, user: str, password: str) -> Dict[str, Any]:
    return {
        "tag": tag,
        "listen": listen,
        "port": int(port),
        "protocol": "socks",
        "settings": {"auth": "password", "accounts": [{"user": user, "pass": password}], "udp": True},
    }
def build_rule(*, rule_tag: str, inbound_tag: str, outbound_tag: str) -> Dict[str, Any]:
    return {"type": "field", "ruleTag": rule_tag, "inboundTag": [inbound_tag], "outboundTag": outbound_tag}
def run_batch(
    *,
    db_path: str,
    data_dir: Path,
    count: int,
    parallel: int,
    port_start: int,
    tag_prefix: str,
    lock_timeout_sec: int,
    check_timeout_sec: int,
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
    with db_connect(db_path) as conn:
        ensure_required(conn)
        cols_links = set(cols(conn, "links"))
        conn.execute("BEGIN IMMEDIATE")
        try:
            ensure_test_inbounds(conn, ports, tag_prefix)
            clear_test_inbounds(conn, ports)
            slots = fetch_test_inbounds(conn, ports)
            links = select_links(conn, cols_links, limit=count, batch_id=batch_id, owner=owner, lock_timeout_sec=lock_timeout_sec)
            n = min(len(slots), len(links))
            slots = slots[:n]
            links = links[:n]
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    if not slots or not links:
        return False, {"status": "idle", "batch_id": batch_id}
    print(f"[{nowlog()}] allocated items={len(links)} ports={slots[0]['port']}..{slots[-1]['port']}")
    sys.stdout.flush()
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
    allocated_pairs: List[Tuple[int, int]] = [(int(s["id"]), int(l["id"])) for s, l in zip(slots, links)]
    released_link_ids: set[int] = set()
    ok_items: List[Dict[str, Any]] = []
    fail_items: List[Dict[str, Any]] = []
    started_at = utc_now()
    with db_connect(db_path) as upd:
        cols_links = set(cols(upd, "links"))
        try:
            jobs: List[Tuple[int, int, int, str, int, str, str]] = []
            for idx, (s, l) in enumerate(zip(slots, links), start=1):
                if _STOP.is_set() or stop_file_exists(stop_file):
                    _set_stop(_STOP_REASON or "stop")
                    break
                link_id = int(l["id"])
                inbound_id = int(s["id"])
                port = int(s["port"])
                inbound_tag = str(s["tag"])
                out_tag = f"xT_{uuid.uuid4().hex[:10]}"
                rule_tag = f"rT_{uuid.uuid4().hex[:10]}"
                try:
                    ob = sanitize_outbound(parse_outbound(str(l["config_json"])))
                    ob["tag"] = out_tag
                except Exception as e:
                    detail = str(e)
                    print(f"[{nowlog()}] FAIL(prep) #{idx} link_id={link_id} code=parse detail={one_line(detail)}")
                    sys.stdout.flush()
                    upd.execute("BEGIN IMMEDIATE")
                    try:
                        update_result(upd, cols_links, link_id=link_id, ok=False, error_code="parse")
                        release_slot(upd, cols_links, inbound_id=inbound_id, link_id=link_id)
                        upd.commit()
                    except Exception:
                        upd.rollback()
                        raise
                    released_link_ids.add(link_id)
                    continue
                r1 = applier.add_outbound(ob)
                if not r1.get("ok"):
                    raw = str(r1.get("stderr") or r1.get("stdout") or "xray_add_outbound_failed")
                    code, mark_unsupported = classify_prep_error(raw)
                    print(f"[{nowlog()}] FAIL(prep) #{idx} link_id={link_id} code={code} detail={one_line(raw)}")
                    sys.stdout.flush()
                    upd.execute("BEGIN IMMEDIATE")
                    try:
                        update_result(upd, cols_links, link_id=link_id, ok=False, error_code=code, mark_proto_unsupported=mark_unsupported)
                        release_slot(upd, cols_links, inbound_id=inbound_id, link_id=link_id)
                        upd.commit()
                    except Exception:
                        upd.rollback()
                        raise
                    released_link_ids.add(link_id)
                    continue
                created_out.append(out_tag)
                r2 = applier.add_inbound(
                    socks_inbound(tag=inbound_tag, listen=socks_listen, port=port, user=socks_user, password=socks_pass)
                )
                if not r2.get("ok"):
                    raw = str(r2.get("stderr") or r2.get("stdout") or "xray_add_inbound_failed")
                    try:
                        applier.remove_outbound(out_tag, ignore_not_found=True)
                    except Exception:
                        pass
                    try:
                        created_out.remove(out_tag)
                    except Exception:
                        pass
                    print(f"[{nowlog()}] FAIL(prep) #{idx} link_id={link_id} code=xray detail={one_line(raw)}")
                    sys.stdout.flush()
                    upd.execute("BEGIN IMMEDIATE")
                    try:
                        update_result(upd, cols_links, link_id=link_id, ok=False, error_code="xray")
                        release_slot(upd, cols_links, inbound_id=inbound_id, link_id=link_id)
                        upd.commit()
                    except Exception:
                        upd.rollback()
                        raise
                    released_link_ids.add(link_id)
                    continue
                created_in.append(inbound_tag)
                rr = applier.apply_rules(
                    {"routing": {"rules": [build_rule(rule_tag=rule_tag, inbound_tag=inbound_tag, outbound_tag=out_tag)]}},
                    append=True,
                )
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
                    try:
                        created_in.remove(inbound_tag)
                    except Exception:
                        pass
                    try:
                        created_out.remove(out_tag)
                    except Exception:
                        pass
                    print(f"[{nowlog()}] FAIL(prep) #{idx} link_id={link_id} code=rule detail={one_line(raw)}")
                    sys.stdout.flush()
                    upd.execute("BEGIN IMMEDIATE")
                    try:
                        update_result(upd, cols_links, link_id=link_id, ok=False, error_code="rule")
                        release_slot(upd, cols_links, inbound_id=inbound_id, link_id=link_id)
                        upd.commit()
                    except Exception:
                        upd.rollback()
                        raise
                    released_link_ids.add(link_id)
                    continue
                created_rules.append(rule_tag)
                upd.execute("BEGIN IMMEDIATE")
                try:
                    bind_slot(upd, cols_links, inbound_id=inbound_id, inbound_tag=inbound_tag, link_id=link_id, out_tag=out_tag)
                    upd.commit()
                except Exception:
                    upd.rollback()
                    raise
                jobs.append((idx, link_id, inbound_id, inbound_tag, port, out_tag, rule_tag))
            if not jobs:
                alive_total = count_is_alive_1(db_path)
                rep = {
                    "status": "ok",
                    "batch_id": batch_id,
                    "summary": {"ok": 0, "fail": 0, "tested": 0, "db_is_alive_1_total": alive_total},
                    "ok": [],
                    "fail": [],
                }
                rp = write_report(data_dir, count=count, batch_id=batch_id, report_file=report_file, report=rep)
                if rp:
                    print(f"[{nowlog()}] REPORT_FILE {rp}")
                print(f"[{nowlog()}] SUMMARY batch_id={batch_id} tested=0 ok=0 fail=0 duration=0.00s db_is_alive_1_total={alive_total}")
                sys.stdout.flush()
                return True, rep
            def do_one(job: Tuple[int, int, int, str, int, str, str]) -> Dict[str, Any]:
                idx, link_id, inbound_id, inbound_tag, port, out_tag, rule_tag = job
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
                res = check_host(check_py, socks5_url=socks5, timeout_sec=check_timeout_sec)
                dur = time.time() - t0
                if str(res.get("status") or "").lower() == "ok":
                    return {
                        "idx": idx,
                        "link_id": link_id,
                        "inbound_id": inbound_id,
                        "port": port,
                        "ok": True,
                        "error": "ok",
                        "error_detail": "",
                        "duration_sec": round(dur, 3),
                        "ip": res.get("IP address"),
                        "country": res.get("Country"),
                        "city": res.get("City"),
                        "isp": res.get("ISP"),
                    }
                code, detail = check_error_code(res)
                return {
                    "idx": idx,
                    "link_id": link_id,
                    "inbound_id": inbound_id,
                    "port": port,
                    "ok": False,
                    "error": code,
                    "error_detail": detail,
                    "duration_sec": round(dur, 3),
                }
            with ThreadPoolExecutor(max_workers=int(parallel)) as ex:
                futs = {ex.submit(do_one, j): j for j in jobs}
                for fut in as_completed(futs):
                    if _STOP.is_set() or stop_file_exists(stop_file):
                        _set_stop(_STOP_REASON or "stop")
                    r = fut.result()
                    link_id = int(r["link_id"])
                    inbound_id = int(r["inbound_id"])
                    ok = bool(r["ok"])
                    if ok:
                        ok_items.append(
                            {
                                "idx": int(r["idx"]),
                                "link_id": link_id,
                                "port": int(r["port"]),
                                "ip": r.get("ip"),
                                "country": r.get("country"),
                                "city": r.get("city"),
                                "isp": r.get("isp"),
                                "duration_sec": r.get("duration_sec"),
                            }
                        )
                        print(
                            f"[{nowlog()}] OK   #{r['idx']}/{len(jobs)} link_id={link_id} port={r['port']} "
                            f"ip={(r.get('ip') or '-')} city={(r.get('city') or '-')} dur={float(r.get('duration_sec') or 0):.2f}s"
                        )
                    else:
                        fail_items.append(
                            {
                                "idx": int(r["idx"]),
                                "link_id": link_id,
                                "port": int(r["port"]),
                                "error": one_word(str(r["error"])),
                                "error_detail": one_line(str(r.get("error_detail") or ""), 240),
                                "duration_sec": r.get("duration_sec"),
                            }
                        )
                        print(
                            f"[{nowlog()}] FAIL #{r['idx']}/{len(jobs)} link_id={link_id} port={r['port']} "
                            f"code={one_word(str(r['error']))} detail={one_line(str(r.get('error_detail') or ''), 240)} "
                            f"dur={float(r.get('duration_sec') or 0):.2f}s"
                        )
                    sys.stdout.flush()
                    upd.execute("BEGIN IMMEDIATE")
                    try:
                        if ok:
                            update_result(
                                upd,
                                cols_links,
                                link_id=link_id,
                                ok=True,
                                error_code="ok",
                                ip=str(r.get("ip") or "").strip() or None,
                                country=str(r.get("country") or "").strip() or None,
                                city=str(r.get("city") or "").strip() or None,
                                isp=str(r.get("isp") or "").strip() or None,
                            )
                        else:
                            update_result(upd, cols_links, link_id=link_id, ok=False, error_code=str(r["error"]))
                        release_slot(upd, cols_links, inbound_id=inbound_id, link_id=link_id)
                        upd.commit()
                    except Exception:
                        upd.rollback()
                        raise
                    released_link_ids.add(link_id)
        finally:
            try:
                if created_rules:
                    applier.remove_rules(created_rules, ignore_not_found=True)
            except Exception:
                pass
            for t in created_in:
                try:
                    applier.remove_inbound(t, ignore_not_found=True)
                except Exception:
                    pass
            for t in created_out:
                try:
                    applier.remove_outbound(t, ignore_not_found=True)
                except Exception:
                    pass
            to_release = [(inb, lid) for inb, lid in allocated_pairs if lid not in released_link_ids]
            if to_release:
                upd.execute("BEGIN IMMEDIATE")
                try:
                    for inb, lid in to_release:
                        release_slot(upd, cols_links, inbound_id=inb, link_id=lid)
                    upd.commit()
                except Exception:
                    upd.rollback()
    finished_at = utc_now()
    dur = (finished_at - started_at).total_seconds()
    alive_total = count_is_alive_1(db_path)
    report = {
        "status": "ok",
        "batch_id": batch_id,
        "db": db_path,
        "count_requested": count,
        "count_tested": len(ok_items) + len(fail_items),
        "parallel": parallel,
        "ports": {"start": port_start, "end": port_start + max(0, (len(slots) - 1))},
        "started_at_utc": iso_utc(started_at),
        "finished_at_utc": iso_utc(finished_at),
        "duration_sec": round(dur, 3),
        "summary": {"ok": len(ok_items), "fail": len(fail_items), "tested": len(ok_items) + len(fail_items), "db_is_alive_1_total": alive_total},
        "ok": ok_items,
        "fail": fail_items,
    }
    rp = write_report(data_dir, count=count, batch_id=batch_id, report_file=report_file, report=report)
    if rp:
        print(f"[{nowlog()}] REPORT_FILE {rp}")
    print(
        f"[{nowlog()}] SUMMARY batch_id={batch_id} tested={len(ok_items) + len(fail_items)} "
        f"ok={len(ok_items)} fail={len(fail_items)} duration={dur:.2f}s db_is_alive_1_total={alive_total}"
    )
    sys.stdout.flush()
    return True, report
def write_report(data_dir: Path, *, count: int, batch_id: str, report_file: str, report: Dict[str, Any]) -> Optional[Path]:
    rf = (report_file or "auto").strip()
    if rf.lower() == "auto":
        rp = data_dir / f"test_report_{count}_{batch_id}.json"
    else:
        rp = Path(rf).expanduser().resolve()
    try:
        rp.parent.mkdir(parents=True, exist_ok=True)
        rp.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return rp
    except Exception:
        return None
def main(argv: Optional[List[str]] = None) -> int:
    try:
        signal.signal(signal.SIGTERM, _sig_handler)
        signal.signal(signal.SIGINT, _sig_handler)
    except Exception:
        pass
    ap = argparse.ArgumentParser(description="xraymgr batch tester (default: continuous until stop)")
    ap.add_argument("--db", default="", help="sqlite db path (default: auto)")
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
    ap.add_argument("--api-server", default="127.0.0.1:10085")
    ap.add_argument("--owner", default="panel")
    ap.add_argument("--run-id", default="")
    ap.add_argument("--data-dir", default="auto", help="auto=dir of db")
    ap.add_argument("--report-file", default="auto")
    ap.add_argument("--stop-file", default="")
    ap.add_argument("--idle-sleep", type=float, default=2.0)
    ap.add_argument("--max-batches", type=int, default=0)
    ap.add_argument("--continuous", action="store_true", help="force continuous (default already continuous)")
    ap.add_argument("--once", action="store_true", help="legacy: single batch then exit")
    args = ap.parse_args(argv)
    continuous = bool(args.continuous) or (not bool(args.once))
    db_path = resolve_db_path(args.db)
    count = max(1, int(args.count or 100))
    parallel = max(1, int(args.parallel or 10))
    run_id = (str(args.run_id).strip() or uuid.uuid4().hex)
    data_cfg = (str(args.data_dir).strip() or "auto")
    data_dir = Path(db_path).expanduser().resolve().parent if data_cfg.lower() == "auto" else Path(data_cfg).expanduser().resolve()
    data_dir.mkdir(parents=True, exist_ok=True)
    stop_file = (str(args.stop_file).strip() or "")
    if stop_file and not os.path.isabs(stop_file):
        stop_file = str(Path(stop_file).expanduser().resolve())
    started = utc_now()
    print(f"[{iso_utc(started).replace('T',' ').replace('Z',' UTC')}] start run_id={run_id} mode={'continuous' if continuous else 'once'} count={count} parallel={parallel} db={db_path}")
    sys.stdout.flush()
    total_ok = 0
    total_fail = 0
    total_tested = 0
    batches = 0
    while True:
        if _STOP.is_set() or stop_file_exists(stop_file):
            _set_stop(_STOP_REASON or "stop")
            break
        if continuous and args.max_batches and batches >= int(args.max_batches):
            break
        batches += 1
        batch_id = f"{run_id}-{batches:06d}" if continuous else run_id
        had, rep = run_batch(
            db_path=db_path,
            data_dir=data_dir,
            count=count,
            parallel=parallel,
            port_start=int(args.port_start or 9000),
            tag_prefix=str(args.inbound_tag_prefix or "in_test_"),
            lock_timeout_sec=int(args.lock_timeout or 90),
            check_timeout_sec=int(args.check_timeout or 60),
            socks_user=str(args.socks_user or "me"),
            socks_pass=str(args.socks_pass or "1"),
            socks_listen=str(args.socks_listen or "127.0.0.1"),
            xray_bin=str(args.xray_bin or "/usr/local/x-ui/bin/xray-linux-amd64"),
            api_server=str(args.api_server or "127.0.0.1:10085"),
            owner=str(args.owner or "panel"),
            batch_id=batch_id,
            report_file=str(args.report_file or "auto"),
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
            if _STOP.is_set() or stop_file_exists(stop_file):
                _set_stop(_STOP_REASON or "stop")
                break
            time.sleep(max(0.2, float(args.idle_sleep)))
            continue
        time.sleep(0.1)
    finished = utc_now()
    dur = (finished - started).total_seconds()
    alive_total = count_is_alive_1(db_path)
    print(
        f"[{iso_utc(finished).replace('T',' ').replace('Z',' UTC')}] GLOBAL_SUMMARY "
        f"run_id={run_id} batches={batches} tested={total_tested} ok={total_ok} fail={total_fail} "
        f"duration={dur:.2f}s stop_reason={_STOP_REASON or ''} db_is_alive_1_total={alive_total}"
    )
    print(f"[{iso_utc(finished).replace('T',' ').replace('Z',' UTC')}] done")
    sys.stdout.flush()
    return 0
if __name__ == "__main__":
    raise SystemExit(main())
