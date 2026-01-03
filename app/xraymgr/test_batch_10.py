#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Batch Test Runner

- count پیش‌فرض: 100 (پورت‌های TEST_PORT_START..TEST_PORT_START+count-1)
- همواره حداکثر 10 تست هم‌زمان (rolling concurrency=10)
- در حین تست لاگ می‌نویسد + در پایان ریپورت JSON چاپ و روی فایل ذخیره می‌کند

تغییرات این نسخه:
1) انتخاب لینک‌ها برای تست فقط از بین primary ها (links.is_config_primary = 1)
2) last_test_error فقط یک کلمه (error code) ذخیره می‌کند؛ جزئیات در لاگ/ریپورت می‌آید.

Sanitizers (safe JSON edits, not string replace):
- TLS fingerprint="none"  -> حذف کلید fingerprint
- RAW/TCP header.type اگر خالی/نامعتبر باشد -> header.type="none"
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


# -----------------------------
# sys.path fix (script is inside package dir)
# -----------------------------
_THIS_FILE = Path(__file__).resolve()
_PKG_DIR = _THIS_FILE.parent          # .../app/xraymgr
_APP_DIR = _PKG_DIR.parent            # .../app
if str(_APP_DIR) not in sys.path:
    sys.path.insert(0, str(_APP_DIR))


# -----------------------------
# Optional project imports
# -----------------------------
try:
    from xraymgr.settings import get_db_path as _get_db_path  # type: ignore
except Exception:
    _get_db_path = None  # type: ignore

try:
    from xraymgr import test_settings as ts  # type: ignore
except Exception:
    ts = None  # type: ignore


# -----------------------------
# Constants
# -----------------------------
PARALLEL_JOBS = 10
MAX_REPLACEMENTS_PER_SLOT = 5


# -----------------------------
# Time helpers (timezone-aware)
# -----------------------------
def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def dt_sqlite(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def clamp_text(s: str, max_len: int = 300) -> str:
    s = (s or "").strip()
    if len(s) <= max_len:
        return s
    return s[: max_len - 3] + "..."


def iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


# -----------------------------
# Logging
# -----------------------------
class Logger:
    def __init__(self, log_path: Path) -> None:
        self.log_path = log_path
        self._fh = open(self.log_path, "a", encoding="utf-8", buffering=1)

    def close(self) -> None:
        try:
            self._fh.close()
        except Exception:
            pass

    def log(self, msg: str) -> None:
        ts_ = utc_now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{ts_} UTC] {msg}"
        print(line)
        try:
            self._fh.write(line + "\n")
        except Exception:
            pass

    def flush(self) -> None:
        try:
            sys.stdout.flush()
        except Exception:
            pass
        try:
            self._fh.flush()
        except Exception:
            pass


# -----------------------------
# Error code policy (DB: one word)
# -----------------------------
def _one_word(code: str) -> str:
    code = (code or "").strip()
    if not code:
        return "fail"
    # remove whitespace, keep first token
    code = code.replace("\n", " ").replace("\r", " ").strip().split()[0]
    # avoid spaces entirely
    return code.replace(" ", "_")[:32] or "fail"


def error_code_from_check_result(res: Dict[str, Any]) -> Tuple[str, str]:
    """
    Returns (code, detail) where:
    - code is one-word suitable for last_test_error
    - detail is human-readable for logs/reports
    """
    et = str(res.get("error_type") or "").strip()
    ed = str(res.get("error_detail") or "").strip()

    if et == "check_host_subprocess_timeout":
        code = "timeout"
    elif et == "check_host_exit_nonzero":
        code = "check"
    elif et == "json_parse_failed":
        code = "badjson"
    elif et == "empty_output":
        code = "empty"
    elif et:
        code = et
    else:
        code = "fail"

    detail = f"{et}:{ed}".strip(":").strip()
    return _one_word(code), clamp_text(detail or json.dumps(res, ensure_ascii=False), 400)


def error_code_from_text(s: str) -> str:
    t = (s or "").strip().lower()
    if "xray_add_outbound_failed" in t or "failed to build" in t or "xray" in t:
        return "xray"
    if "outbound_config_parse_failed" in t or "config_parse" in t or "json" in t:
        return "parse"
    if "slot_replacement_exhausted" in t or "exhaust" in t:
        return "exhaust"
    return "fail"


# -----------------------------
# DB helpers
# -----------------------------
def resolve_db_path(cli_db: Optional[str]) -> str:
    if cli_db and str(cli_db).strip():
        return str(cli_db).strip()

    env = os.environ.get("XRAYMGR_DB_PATH")
    if env and env.strip():
        return env.strip()

    if _get_db_path:
        try:
            p = str(_get_db_path()).strip()
            if p:
                return p
        except Exception:
            pass

    opt_default = Path("/opt/xraymgr/data/xraymgr.db")
    if opt_default.exists():
        return str(opt_default)

    repo_root = _THIS_FILE.parents[2]
    return str((repo_root / "data" / "xraymgr.db").resolve())


def connect_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def db_database_list(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    rows = conn.execute("PRAGMA database_list").fetchall()
    return [{"seq": r[0], "name": r[1], "file": r[2]} for r in rows]


def ensure_required_tables(conn: sqlite3.Connection) -> None:
    tables = {
        str(r[0])
        for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    }
    missing = [t for t in ("inbound", "links") if t not in tables]
    if missing:
        raise RuntimeError(f"DB missing required tables: {missing}")


def table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    cols = []
    for r in conn.execute(f"PRAGMA table_info({table})").fetchall():
        cols.append(str(r[1]))
    return cols


# -----------------------------
# Xray CLI wrapper (xray api ...)
# -----------------------------
DEFAULT_XRAY_BIN_CANDIDATES: Tuple[str, ...] = (
    "/usr/local/x-ui/bin/xray-linux-amd64",
    "/usr/local/bin/xray",
    "/usr/bin/xray",
    "xray",
)

DEFAULT_API_SERVER_CANDIDATES: Tuple[str, ...] = (
    "127.0.0.1:10085",
    "127.0.0.1:8080",
    "127.0.0.1:11111",
)


@dataclass(frozen=True)
class CmdResult:
    rc: int
    stdout: str
    stderr: str

    @property
    def ok(self) -> bool:
        return self.rc == 0


def _first_existing_executable(candidates: Sequence[str]) -> Optional[str]:
    for c in candidates:
        if os.path.sep not in c:
            resolved = shutil.which(c)
            if resolved:
                return resolved
            continue
        if os.path.exists(c) and os.access(c, os.X_OK):
            return c
    return None


def _try_parse_json(s: str) -> Optional[Any]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        return json.loads(s)
    except Exception:
        return None


def _looks_like_not_found(text: str) -> bool:
    t = (text or "").strip().upper()
    return "NOT_FOUND" in t or "NOTFOUND" in t or "NOT FOUND" in t


class XrayCli:
    def __init__(
        self,
        xray_bin: Optional[str],
        api_server: str = "auto",
        *,
        timeout_sec: float = 30.0,
        probe_timeout_sec: float = 3.0,
    ) -> None:
        self.xray_bin = xray_bin or _first_existing_executable(DEFAULT_XRAY_BIN_CANDIDATES)
        if not self.xray_bin:
            raise RuntimeError("XRAY binary not found. Set --xray-bin.")
        self.timeout_sec = float(timeout_sec)
        self.probe_timeout_sec = float(probe_timeout_sec)

        if str(api_server).strip().lower() == "auto":
            self.api_server = self.probe_api_server(DEFAULT_API_SERVER_CANDIDATES)
        else:
            self.api_server = str(api_server).strip()

    def _run(self, subcommand: str, args: Optional[Sequence[str]] = None, *, timeout: Optional[float] = None) -> CmdResult:
        cmd = [self.xray_bin, "api", str(subcommand), f"--server={self.api_server}"]
        if args:
            cmd.extend([str(a) for a in args])
        try:
            p = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=float(timeout if timeout is not None else self.timeout_sec),
            )
            return CmdResult(p.returncode, (p.stdout or "").strip(), (p.stderr or "").strip())
        except subprocess.TimeoutExpired:
            return CmdResult(124, "", f"timeout running command: {cmd!r}")
        except Exception as e:
            return CmdResult(1, "", f"failed running command: {e}")

    def _run_with_server(self, subcommand: str, *, server: str, timeout: float) -> CmdResult:
        cmd = [self.xray_bin, "api", str(subcommand), f"--server={server}"]
        try:
            p = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=float(timeout),
            )
            return CmdResult(p.returncode, (p.stdout or "").strip(), (p.stderr or "").strip())
        except subprocess.TimeoutExpired:
            return CmdResult(124, "", f"timeout running command: {cmd!r}")
        except Exception as e:
            return CmdResult(1, "", f"failed running command: {e}")

    def probe_api_server(self, candidates: Sequence[str]) -> str:
        for srv in candidates:
            r = self._run_with_server("lso", server=srv, timeout=self.probe_timeout_sec)
            if r.ok and _try_parse_json(r.stdout) is not None:
                return str(srv)
        return str(candidates[0])

    def _run_with_temp_json(self, subcommand: str, obj: Dict[str, Any], extra_args: Optional[Sequence[str]] = None) -> CmdResult:
        path = None
        try:
            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8") as f:
                json.dump(obj, f, ensure_ascii=False)
                path = f.name
            args = list(extra_args or [])
            args.append(path)
            return self._run(subcommand, args=args)
        finally:
            if path and os.path.exists(path):
                try:
                    os.remove(path)
                except Exception:
                    pass

    def list_inbounds(self) -> Dict[str, Any]:
        r = self._run("lsi")
        data = _try_parse_json(r.stdout)
        return {"ok": r.ok and data is not None, "rc": r.rc, "stdout": r.stdout, "stderr": r.stderr, "data": data}

    def list_outbounds(self) -> Dict[str, Any]:
        r = self._run("lso")
        data = _try_parse_json(r.stdout)
        return {"ok": r.ok and data is not None, "rc": r.rc, "stdout": r.stdout, "stderr": r.stderr, "data": data}

    def list_inbound_tags(self) -> List[str]:
        r = self.list_inbounds()
        if not r.get("ok"):
            return []
        ibs = (r.get("data") or {}).get("inbounds") or []
        tags: List[str] = []
        for ib in ibs:
            if isinstance(ib, dict) and ib.get("tag"):
                tags.append(str(ib["tag"]))
        return tags

    def list_outbound_tags(self) -> List[str]:
        r = self.list_outbounds()
        if not r.get("ok"):
            return []
        obs = (r.get("data") or {}).get("outbounds") or []
        tags: List[str] = []
        for ob in obs:
            if isinstance(ob, dict) and ob.get("tag"):
                tags.append(str(ob["tag"]))
        return tags

    def add_inbound(self, inbound_obj: Dict[str, Any]) -> CmdResult:
        return self._run_with_temp_json("adi", {"inbounds": [inbound_obj]})

    def add_outbound(self, outbound_obj: Dict[str, Any]) -> CmdResult:
        return self._run_with_temp_json("ado", {"outbounds": [outbound_obj]})

    def remove_inbound(self, tag: str, *, ignore_not_found: bool = True) -> CmdResult:
        r = self._run("rmi", args=[str(tag)])
        if r.ok:
            return r
        if ignore_not_found and (_looks_like_not_found(r.stdout) or _looks_like_not_found(r.stderr)):
            return CmdResult(0, r.stdout, r.stderr)
        return r

    def remove_outbound(self, tag: str, *, ignore_not_found: bool = True) -> CmdResult:
        r = self._run("rmo", args=[str(tag)])
        if r.ok:
            return r
        if ignore_not_found and (_looks_like_not_found(r.stdout) or _looks_like_not_found(r.stderr)):
            return CmdResult(0, r.stdout, r.stderr)
        return r

    def remove_rules(self, rule_tags: Iterable[str], *, ignore_not_found: bool = True) -> List[CmdResult]:
        out: List[CmdResult] = []
        for t in rule_tags:
            t2 = str(t).strip()
            if not t2:
                continue
            r = self._run("rmrules", args=[t2])
            if r.ok or (ignore_not_found and (_looks_like_not_found(r.stdout) or _looks_like_not_found(r.stderr))):
                out.append(CmdResult(0, r.stdout, r.stderr))
            else:
                out.append(r)
        return out

    def apply_rules(self, routing_obj: Dict[str, Any], *, append: bool) -> CmdResult:
        payload = routing_obj if "routing" in routing_obj else {"routing": routing_obj}
        extra = ["-append"] if append else []
        return self._run_with_temp_json("adrules", payload, extra_args=extra)


# -----------------------------
# DB logic: pool, lock, bind
# -----------------------------
def ensure_test_inbounds(conn: sqlite3.Connection, ports: List[int], tag_prefix: str) -> None:
    for p in ports:
        tag = f"{tag_prefix}{p}"
        conn.execute(
            """
            INSERT OR IGNORE INTO inbound (role, is_active, port, tag, link_id, outbound_tag, status, last_test_at)
            VALUES ('test', 0, ?, ?, NULL, NULL, 'new', NULL)
            """,
            (int(p), tag),
        )


def clear_test_inbounds(conn: sqlite3.Connection, ports: List[int]) -> None:
    q = ",".join(["?"] * len(ports))
    conn.execute(
        f"""
        UPDATE inbound
        SET link_id=NULL, outbound_tag=NULL, is_active=0, status='new'
        WHERE role='test' AND port IN ({q})
        """,
        [int(p) for p in ports],
    )


def fetch_test_inbounds(conn: sqlite3.Connection, ports: List[int], tag_prefix: str) -> List[sqlite3.Row]:
    q = ",".join(["?"] * len(ports))
    rows = conn.execute(
        f"""
        SELECT id, port, tag
        FROM inbound
        WHERE role='test' AND port IN ({q})
        ORDER BY port ASC
        """,
        [int(p) for p in ports],
    ).fetchall()

    got = {int(r["port"]) for r in rows}
    missing = [p for p in ports if int(p) not in got]
    if missing:
        raise RuntimeError(f"Missing test inbounds for ports: {missing}")

    for r in rows:
        expected = f"{tag_prefix}{int(r['port'])}"
        if str(r["tag"]) != expected:
            raise RuntimeError(f"Inbound tag mismatch for port {r['port']}: got={r['tag']!r} expected={expected!r}")

    return rows


def select_links_for_test(
    conn: sqlite3.Connection,
    limit: int,
    now: datetime,
    lock_timeout_sec: int,
    owner: str,
    batch_id: str,
) -> List[sqlite3.Row]:
    now_s = dt_sqlite(now)
    lock_until_s = dt_sqlite(now + timedelta(seconds=int(lock_timeout_sec)))

    rows = conn.execute(
        """
        SELECT id, url, config_json, outbound_tag
        FROM links
        WHERE
          COALESCE(is_config_primary, 0) = 1
          AND last_test_at IS NULL
          AND (outbound_tag IS NOT NULL AND TRIM(outbound_tag) <> '')
          AND (config_json IS NOT NULL AND TRIM(config_json) <> '')
          AND is_invalid = 0
          AND is_protocol_unsupported = 0
          AND (
            test_status = 'idle'
            OR (
              test_status = 'running'
              AND (test_lock_until IS NULL OR test_lock_until < ?)
            )
          )
        ORDER BY id ASC
        LIMIT ?
        """,
        (now_s, int(limit)),
    ).fetchall()

    if len(rows) < limit:
        raise RuntimeError(f"Not enough untested PRIMARY links for batch. needed={limit} got={len(rows)}")

    ids = [int(r["id"]) for r in rows]
    q = ",".join(["?"] * len(ids))

    cur = conn.execute(
        f"""
        UPDATE links
        SET test_status='running',
            test_started_at=?,
            test_lock_until=?,
            test_lock_owner=?,
            test_batch_id=?
        WHERE id IN ({q})
          AND last_test_at IS NULL
          AND COALESCE(is_config_primary, 0) = 1
          AND (
            test_status = 'idle'
            OR (
              test_status = 'running'
              AND (test_lock_until IS NULL OR test_lock_until < ?)
            )
          )
        """,
        [now_s, lock_until_s, owner, batch_id, *ids, now_s],
    )
    if cur.rowcount != len(ids):
        raise RuntimeError("Atomic reserve failed (race). Retry later.")

    return rows


def bind_inbound_to_link(
    conn: sqlite3.Connection,
    *,
    inbound_id: int,
    inbound_tag: str,
    port: int,
    link_row: sqlite3.Row,
) -> None:
    link_id = int(link_row["id"])
    outbound_tag = str(link_row["outbound_tag"]).strip()

    conn.execute(
        """
        UPDATE inbound
        SET link_id=?, outbound_tag=?, status='bound'
        WHERE id=?
        """,
        (link_id, outbound_tag, int(inbound_id)),
    )

    conn.execute(
        """
        UPDATE links
        SET inbound_tag=?,
            is_in_use=1,
            bound_port=?
        WHERE id=?
        """,
        (str(inbound_tag), int(port), link_id),
    )


def bind_inbounds_to_links(
    conn: sqlite3.Connection,
    inbounds: List[sqlite3.Row],
    links: List[sqlite3.Row],
) -> List[Tuple[sqlite3.Row, sqlite3.Row]]:
    pairs = list(zip(inbounds, links))
    for ib, lk in pairs:
        bind_inbound_to_link(
            conn,
            inbound_id=int(ib["id"]),
            inbound_tag=str(ib["tag"]).strip(),
            port=int(ib["port"]),
            link_row=lk,
        )
    return pairs


def release_batch_locks(conn: sqlite3.Connection, batch_id: str, ports: List[int]) -> None:
    conn.execute(
        """
        UPDATE links
        SET test_status='idle',
            test_started_at=NULL,
            test_lock_until=NULL,
            test_lock_owner=NULL,
            test_batch_id=NULL
        WHERE test_batch_id=?
        """,
        (str(batch_id),),
    )
    clear_test_inbounds(conn, ports)


# -----------------------------
# Config builders (Xray)
# -----------------------------
def build_socks_inbound(tag: str, port: int, listen: str, user: str, password: str) -> Dict[str, Any]:
    return {
        "tag": tag,
        "listen": listen,
        "port": int(port),
        "protocol": "socks",
        "settings": {
            "auth": "password",
            "accounts": [{"user": user, "pass": password}],
            "udp": True,
            "ip": listen,
            "userLevel": 0,
        },
    }


def _sanitize_fingerprint_inplace(obj: Any) -> None:
    if isinstance(obj, dict):
        fp = obj.get("fingerprint", None)
        if isinstance(fp, str) and fp.strip().lower() == "none":
            obj.pop("fingerprint", None)
        for v in list(obj.values()):
            _sanitize_fingerprint_inplace(v)
    elif isinstance(obj, list):
        for v in obj:
            _sanitize_fingerprint_inplace(v)


def _normalize_raw_header_object(h: Any) -> Dict[str, Any]:
    if h is None:
        return {"type": "none"}

    if isinstance(h, str):
        t = h.strip().lower()
        if t == "http":
            return {"type": "http", "request": {}, "response": {}}
        return {"type": "none"}

    if isinstance(h, dict):
        t = h.get("type", None)
        if not isinstance(t, str) or not t.strip():
            return {"type": "none"}
        t2 = t.strip().lower()
        if t2 == "none":
            return {"type": "none"}
        if t2 == "http":
            req = h.get("request", {})
            resp = h.get("response", {})
            if not isinstance(req, dict):
                req = {}
            if not isinstance(resp, dict):
                resp = {}
            return {"type": "http", "request": req, "response": resp}
        return {"type": "none"}

    return {"type": "none"}


def _sanitize_stream_settings_inplace(obj: Any) -> None:
    if not isinstance(obj, dict):
        return

    ss = obj.get("streamSettings")
    if not isinstance(ss, dict):
        return

    for key in ("rawSettings", "tcpSettings"):
        st = ss.get(key)
        if st is None:
            continue
        if not isinstance(st, dict):
            ss.pop(key, None)
            continue

        if "header" in st:
            st["header"] = _normalize_raw_header_object(st.get("header"))


def normalize_outbound_from_config_json(config_json: str, outbound_tag: str) -> Dict[str, Any]:
    obj = json.loads(config_json)
    outbound: Optional[Dict[str, Any]] = None

    if isinstance(obj, dict) and "protocol" in obj:
        outbound = obj
    elif isinstance(obj, dict) and isinstance(obj.get("outbounds"), list) and obj["outbounds"]:
        first = obj["outbounds"][0]
        if isinstance(first, dict) and "protocol" in first:
            outbound = first
    elif isinstance(obj, dict) and isinstance(obj.get("outbound"), dict) and "protocol" in obj["outbound"]:
        outbound = obj["outbound"]

    if not outbound or not isinstance(outbound, dict):
        raise ValueError("config_json does not look like an outbound object")

    outbound["tag"] = str(outbound_tag).strip()
    _sanitize_fingerprint_inplace(outbound)
    _sanitize_stream_settings_inplace(outbound)
    return outbound


def build_test_routing_rules(slots: List["Slot"]) -> Tuple[List[Dict[str, Any]], List[str]]:
    rules: List[Dict[str, Any]] = []
    rule_tags: List[str] = []
    for s in slots:
        rt = f"rt_test_{s.inbound_tag}"
        rule_tags.append(rt)
        rules.append(
            {
                "type": "field",
                "inboundTag": [s.inbound_tag],
                "outboundTag": s.outbound_tag,
                "ruleTag": rt,
            }
        )
    return rules, rule_tags


# -----------------------------
# Test execution (via check-host.py)
# -----------------------------
def run_check_host(
    check_host_path: Path,
    *,
    socks_host: str,
    socks_port: int,
    user: str,
    password: str,
    timeout_sec: int,
) -> Dict[str, Any]:
    proxy = f"socks5h://{user}:{password}@{socks_host}:{int(socks_port)}"
    cmd = [
        sys.executable,
        str(check_host_path),
        "--socks5",
        proxy,
        "--timeout",
        str(int(timeout_sec)),
    ]

    outer_timeout = float(max(5, int(timeout_sec) + 15))
    try:
        p = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=outer_timeout,
        )
    except subprocess.TimeoutExpired:
        return {
            "status": "error",
            "error_type": "check_host_subprocess_timeout",
            "error_detail": f"outer_timeout={outer_timeout}s",
        }

    out = (p.stdout or "").strip()
    if p.returncode != 0:
        return {
            "status": "error",
            "error_type": "check_host_exit_nonzero",
            "error_detail": clamp_text(p.stderr or out or f"exit={p.returncode}", 400),
            "_raw": clamp_text(out, 400),
        }
    try:
        return json.loads(out) if out else {"status": "error", "error_type": "empty_output", "error_detail": "no stdout"}
    except Exception as e:
        return {"status": "error", "error_type": "json_parse_failed", "error_detail": str(e), "_raw": clamp_text(out, 400)}


def is_test_ok(result: Dict[str, Any]) -> bool:
    if not isinstance(result, dict):
        return False
    if result.get("status") != "ok":
        return False
    ip = result.get("IP address")
    return bool(ip and str(ip).strip())


def extract_success_metadata(result: Dict[str, Any]) -> Dict[str, Optional[str]]:
    if not isinstance(result, dict):
        return {"ip": None, "country": None, "city": None, "datacenter": None}

    def norm(v: Any) -> Optional[str]:
        if v is None:
            return None
        s = str(v).strip()
        return s if s else None

    return {
        "ip": norm(result.get("IP address")),
        "country": norm(result.get("Country")),
        "city": norm(result.get("City")),
        "datacenter": norm(result.get("ISP")),
    }


@dataclass(frozen=True)
class TestJob:
    idx: int
    inbound_id: int
    inbound_tag: str
    port: int
    link_id: int
    outbound_tag: str


@dataclass
class TestResult:
    job: TestJob
    ok: bool
    started_at: datetime
    finished_at: datetime
    duration_sec: float
    meta: Optional[Dict[str, Optional[str]]]
    err_code: str          # one word (DB)
    err_detail: str        # human detail (logs/reports)
    raw: Dict[str, Any]


def _run_one_test(
    job: TestJob,
    *,
    check_host_path: Path,
    socks_listen: str,
    socks_user: str,
    socks_pass: str,
    check_timeout: int,
) -> TestResult:
    st = utc_now()
    res = run_check_host(
        check_host_path,
        socks_host=socks_listen,
        socks_port=job.port,
        user=socks_user,
        password=socks_pass,
        timeout_sec=check_timeout,
    )

    ok = is_test_ok(res)
    meta = extract_success_metadata(res) if ok else None

    err_code = ""
    err_detail = ""
    if not ok:
        err_code, err_detail = error_code_from_check_result(res)

    ft = utc_now()
    dur = max(0.0, (ft - st).total_seconds())
    return TestResult(
        job=job,
        ok=ok,
        started_at=st,
        finished_at=ft,
        duration_sec=dur,
        meta=meta,
        err_code=err_code,
        err_detail=err_detail,
        raw=res if isinstance(res, dict) else {"status": "error", "error_type": "non_dict_result"},
    )


# -----------------------------
# DB finalize
# -----------------------------
def finalize_db_after_test(
    conn: sqlite3.Connection,
    *,
    links_cols: set[str],
    inbound_id: int,
    link_id: int,
    ok: bool,
    err_code: str,
    now: datetime,
    meta: Optional[Dict[str, Optional[str]]] = None,
) -> None:
    now_s = dt_sqlite(now)
    ok_i = 1 if ok else 0

    last_err_val: Optional[str] = None
    if not ok:
        last_err_val = _one_word(err_code)

    ip: Optional[str] = None
    country: Optional[str] = None
    city: Optional[str] = None
    datacenter: Optional[str] = None
    if ok and meta:
        ip = meta.get("ip") or None
        country = meta.get("country") or None
        city = meta.get("city") or None
        datacenter = meta.get("datacenter") or None

    set_parts = [
        "last_test_at=?",
        "last_test_ok=?",
        "last_test_error=?",
        "is_alive=?",
        "test_status='idle'",
        "test_started_at=NULL",
        "test_lock_until=NULL",
        "test_lock_owner=NULL",
        "test_batch_id=NULL",
        "inbound_tag=NULL",
        "is_in_use=0",
        "bound_port=NULL",
    ]
    params: List[Any] = [now_s, ok_i, last_err_val, ok_i]

    if ok and "ip" in links_cols:
        set_parts.insert(4, "ip=COALESCE(?, ip)")
        params.insert(4, ip)
    if ok and "country" in links_cols:
        set_parts.insert(4, "country=COALESCE(?, country)")
        params.insert(4, country)
    if ok and "city" in links_cols:
        set_parts.insert(4, "city=COALESCE(?, city)")
        params.insert(4, city)
    if ok and "datacenter" in links_cols:
        set_parts.insert(4, "datacenter=COALESCE(?, datacenter)")
        params.insert(4, datacenter)

    sql = f"UPDATE links SET {', '.join(set_parts)} WHERE id=?"
    params.append(int(link_id))
    conn.execute(sql, params)

    conn.execute(
        """
        UPDATE inbound
        SET last_test_at=?,
            link_id=NULL,
            outbound_tag=NULL,
            is_active=0,
            status='idle'
        WHERE id=?
        """,
        (now_s, int(inbound_id)),
    )


# -----------------------------
# Slot model (mutable binding)
# -----------------------------
@dataclass
class Slot:
    idx: int
    inbound_id: int
    inbound_tag: str
    port: int
    link_id: int
    outbound_tag: str
    config_json: str
    url: str


# -----------------------------
# Replacement logic (keep count stable)
# -----------------------------
def mark_link_failed_and_free_inbound(
    db_path: str,
    *,
    links_cols: set[str],
    inbound_id: int,
    link_id: int,
    error_text: str,
) -> None:
    code = error_code_from_text(error_text)
    conn = connect_db(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        finalize_db_after_test(
            conn,
            links_cols=links_cols,
            inbound_id=int(inbound_id),
            link_id=int(link_id),
            ok=False,
            err_code=code,
            now=utc_now(),
            meta=None,
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def reserve_and_bind_replacement(
    db_path: str,
    *,
    inbound_id: int,
    inbound_tag: str,
    port: int,
    owner: str,
    batch_id: str,
    lock_timeout_sec: int,
) -> sqlite3.Row:
    conn = connect_db(db_path)
    try:
        now = utc_now()
        conn.execute("BEGIN IMMEDIATE")
        rows = select_links_for_test(
            conn,
            limit=1,
            now=now,
            lock_timeout_sec=int(lock_timeout_sec),
            owner=owner,
            batch_id=batch_id,
        )
        link_row = rows[0]
        bind_inbound_to_link(
            conn,
            inbound_id=int(inbound_id),
            inbound_tag=str(inbound_tag),
            port=int(port),
            link_row=link_row,
        )
        conn.commit()
        return link_row
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# -----------------------------
# Main
# -----------------------------
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=None, help="SQLite DB path. If omitted, uses XRAYMGR_DB_PATH or settings.get_db_path().")
    ap.add_argument("--count", type=int, default=100, help="How many PRIMARY links/inbounds to test (default 100).")
    ap.add_argument("--xray-bin", default="auto", help="Path to xray binary or 'auto'")
    ap.add_argument("--api-server", default="auto", help="Xray API server host:port or 'auto'")
    ap.add_argument("--socks-listen", default="127.0.0.1", help="Listen address for test SOCKS inbounds (default 127.0.0.1)")
    ap.add_argument("--check-timeout", type=int, default=20, help="check-host request timeout (seconds)")
    ap.add_argument("--log-file", default="auto", help="Log file path or 'auto'")
    ap.add_argument("--report-file", default="auto", help="Report JSON path or 'auto'")
    args = ap.parse_args()

    count = int(args.count)
    if count <= 0:
        raise SystemExit("--count must be > 0")
    if count < PARALLEL_JOBS:
        raise SystemExit(f"--count must be >= {PARALLEL_JOBS} to keep 10 jobs always running")
    if count > 500:
        raise SystemExit("--count too large for this runner (max 500)")

    db_path = resolve_db_path(args.db)
    if not os.path.exists(db_path):
        raise SystemExit(f"DB file not found: {db_path}")

    # settings defaults
    test_port_start = 9000
    lock_timeout_sec = 60
    tag_prefix = "in_test_"
    socks_user = "me"
    socks_pass = "1"

    if ts is not None:
        test_port_start = int(getattr(ts, "TEST_PORT_START", test_port_start))
        lock_timeout_sec = int(getattr(ts, "TEST_LOCK_TIMEOUT_SECONDS", lock_timeout_sec))
        tag_prefix = str(getattr(ts, "TEST_INBOUND_TAG_PREFIX", tag_prefix))
        socks_user = str(getattr(ts, "TEST_SOCKS_USER", socks_user))
        socks_pass = str(getattr(ts, "TEST_SOCKS_PASS", socks_pass))

    ports = [test_port_start + i for i in range(count)]
    owner = f"{socket.gethostname()}:{os.getpid()}"
    batch_id = str(uuid.uuid4())
    started_at = utc_now()

    # files
    data_dir = Path("/opt/xraymgr/data")
    if not data_dir.exists():
        data_dir = Path(db_path).parent

    if str(args.log_file).strip().lower() == "auto":
        log_path = data_dir / f"test_batch_{count}_{batch_id}.log"
    else:
        log_path = Path(str(args.log_file).strip())

    if str(args.report_file).strip().lower() == "auto":
        report_path = data_dir / f"test_report_{count}_{batch_id}.json"
    else:
        report_path = Path(str(args.report_file).strip())

    logger = Logger(log_path)
    logger.log(f"start batch_id={batch_id} count={count} parallel={PARALLEL_JOBS} db={db_path}")

    check_host_path = (_PKG_DIR / "check-host.py").resolve()
    if not check_host_path.exists():
        logger.log(f"ERROR check-host.py not found at: {check_host_path}")
        logger.close()
        raise SystemExit(2)

    # DB diagnostics + columns
    cdbg = connect_db(db_path)
    try:
        ensure_required_tables(cdbg)
        logger.log(f"db_database_list={json.dumps(db_database_list(cdbg), ensure_ascii=False)}")
        links_cols = set(table_columns(cdbg, "links"))
    finally:
        cdbg.close()

    # -----------------------------
    # DB: allocate initial batch (atomic)
    # -----------------------------
    initial_pairs: List[Tuple[sqlite3.Row, sqlite3.Row]] = []
    conn = connect_db(db_path)
    try:
        now = utc_now()
        conn.execute("BEGIN IMMEDIATE")

        ensure_test_inbounds(conn, ports, tag_prefix)
        clear_test_inbounds(conn, ports)
        inbounds = fetch_test_inbounds(conn, ports, tag_prefix)

        links = select_links_for_test(
            conn,
            limit=count,
            now=now,
            lock_timeout_sec=lock_timeout_sec,
            owner=owner,
            batch_id=batch_id,
        )
        initial_pairs = bind_inbounds_to_links(conn, inbounds, links)

        conn.commit()
        logger.log(f"allocated pairs={len(initial_pairs)}")
    except Exception as e:
        conn.rollback()
        logger.log(f"ERROR allocate/bind failed: {e}")
        logger.close()
        raise
    finally:
        conn.close()

    # Convert to mutable slots
    slots: List[Slot] = []
    for i, (ib, lk) in enumerate(initial_pairs, start=1):
        slots.append(
            Slot(
                idx=i,
                inbound_id=int(ib["id"]),
                inbound_tag=str(ib["tag"]).strip(),
                port=int(ib["port"]),
                link_id=int(lk["id"]),
                outbound_tag=str(lk["outbound_tag"]).strip(),
                config_json=str(lk["config_json"]),
                url=str(lk["url"] or ""),
            )
        )

    # -----------------------------
    # XRAY: connect + preflight add outbounds (with replacement to keep count)
    # -----------------------------
    xray_bin = None if str(args.xray_bin).strip().lower() == "auto" else str(args.xray_bin).strip()
    xr = XrayCli(xray_bin=xray_bin, api_server=str(args.api_server), timeout_sec=45.0, probe_timeout_sec=3.0)

    existing_in = set(xr.list_inbound_tags())
    existing_out = set(xr.list_outbound_tags())

    created_in: List[str] = []
    created_out: List[str] = []
    rule_tags: List[str] = []

    try:
        # preflight: ensure all outbounds exist; if a link's outbound cannot be added, mark failed and replace link for that slot.
        for s in slots:
            replacements = 0
            while True:
                otag = s.outbound_tag
                if otag in existing_out:
                    break

                try:
                    outbound_obj = normalize_outbound_from_config_json(s.config_json, otag)
                except Exception as e:
                    err_text = f"outbound_config_parse_failed link_id={s.link_id} tag={otag} err={e}"
                    logger.log(f"FAIL(preflight) {err_text}")
                    mark_link_failed_and_free_inbound(
                        db_path,
                        links_cols=links_cols,
                        inbound_id=s.inbound_id,
                        link_id=s.link_id,
                        error_text=err_text,
                    )
                    replacements += 1
                    if replacements > MAX_REPLACEMENTS_PER_SLOT:
                        raise RuntimeError(f"slot_replacement_exhausted inbound={s.inbound_tag}")
                    repl = reserve_and_bind_replacement(
                        db_path,
                        inbound_id=s.inbound_id,
                        inbound_tag=s.inbound_tag,
                        port=s.port,
                        owner=owner,
                        batch_id=batch_id,
                        lock_timeout_sec=lock_timeout_sec,
                    )
                    s.link_id = int(repl["id"])
                    s.outbound_tag = str(repl["outbound_tag"]).strip()
                    s.config_json = str(repl["config_json"])
                    s.url = str(repl["url"] or "")
                    logger.log(f"replace slot={s.idx} inbound={s.inbound_tag} -> link_id={s.link_id} outbound={s.outbound_tag}")
                    continue

                r = xr.add_outbound(outbound_obj)
                if r.ok:
                    created_out.append(otag)
                    existing_out.add(otag)
                    break

                err_text = f"xray_add_outbound_failed link_id={s.link_id} tag={otag} rc={r.rc} stderr={r.stderr}"
                logger.log(f"FAIL(preflight) {err_text}")
                mark_link_failed_and_free_inbound(
                    db_path,
                    links_cols=links_cols,
                    inbound_id=s.inbound_id,
                    link_id=s.link_id,
                    error_text=err_text,
                )
                replacements += 1
                if replacements > MAX_REPLACEMENTS_PER_SLOT:
                    raise RuntimeError(f"slot_replacement_exhausted inbound={s.inbound_tag}")

                repl = reserve_and_bind_replacement(
                    db_path,
                    inbound_id=s.inbound_id,
                    inbound_tag=s.inbound_tag,
                    port=s.port,
                    owner=owner,
                    batch_id=batch_id,
                    lock_timeout_sec=lock_timeout_sec,
                )
                s.link_id = int(repl["id"])
                s.outbound_tag = str(repl["outbound_tag"]).strip()
                s.config_json = str(repl["config_json"])
                s.url = str(repl["url"] or "")
                logger.log(f"replace slot={s.idx} inbound={s.inbound_tag} -> link_id={s.link_id} outbound={s.outbound_tag}")

        logger.log("preflight outbounds ready")

        # add inbounds if missing
        for i, s in enumerate(slots, start=1):
            itag = s.inbound_tag
            if itag in existing_in:
                continue
            inbound_obj = build_socks_inbound(
                tag=itag,
                port=int(s.port),
                listen=str(args.socks_listen),
                user=socks_user,
                password=socks_pass,
            )
            r = xr.add_inbound(inbound_obj)
            if not r.ok:
                raise RuntimeError(f"XRAY add_inbound failed tag={itag} rc={r.rc} stderr={r.stderr}")
            created_in.append(itag)
            existing_in.add(itag)
            if i % 25 == 0:
                logger.log(f"xray inbounds added progress={i}/{count}")

        # routing: cleanup same ruleTags then append
        rules, rule_tags = build_test_routing_rules(slots)
        xr.remove_rules(rule_tags)  # best-effort
        rr = xr.apply_rules({"routing": {"rules": rules}}, append=True)
        if not rr.ok:
            raise RuntimeError(f"XRAY adrules failed rc={rr.rc} stderr={rr.stderr}")

        logger.log("xray apply completed")

        # -----------------------------
        # TEST: rolling concurrency = 10 until completion
        # -----------------------------
        jobs: List[TestJob] = [
            TestJob(
                idx=s.idx,
                inbound_id=s.inbound_id,
                inbound_tag=s.inbound_tag,
                port=s.port,
                link_id=s.link_id,
                outbound_tag=s.outbound_tag,
            )
            for s in slots
        ]

        ok_items: List[Dict[str, Any]] = []
        fail_items: List[Dict[str, Any]] = []

        logger.log(f"test start: jobs={len(jobs)} parallel={PARALLEL_JOBS}")

        connw = connect_db(db_path)
        try:
            next_idx = 0
            in_flight: Dict[Any, TestJob] = {}

            def submit_next(ex: ThreadPoolExecutor) -> bool:
                nonlocal next_idx
                if next_idx >= len(jobs):
                    return False
                job = jobs[next_idx]
                next_idx += 1
                fut = ex.submit(
                    _run_one_test,
                    job,
                    check_host_path=check_host_path,
                    socks_listen=str(args.socks_listen),
                    socks_user=socks_user,
                    socks_pass=socks_pass,
                    check_timeout=int(args.check_timeout),
                )
                in_flight[fut] = job
                logger.log(f"queued #{job.idx}/{len(jobs)} link_id={job.link_id} outbound={job.outbound_tag} port={job.port}")
                return True

            with ThreadPoolExecutor(max_workers=PARALLEL_JOBS) as ex:
                for _ in range(min(PARALLEL_JOBS, len(jobs))):
                    submit_next(ex)

                completed = 0
                while in_flight:
                    for fut in as_completed(list(in_flight.keys())):
                        job = in_flight.pop(fut)
                        try:
                            tr: TestResult = fut.result()
                        except Exception as e:
                            tr = TestResult(
                                job=job,
                                ok=False,
                                started_at=utc_now(),
                                finished_at=utc_now(),
                                duration_sec=0.0,
                                meta=None,
                                err_code="worker",
                                err_detail=str(e),
                                raw={"status": "error", "error_type": "worker_exception", "error_detail": str(e)},
                            )

                        completed += 1

                        # DB write (single-writer in main thread)
                        connw.execute("BEGIN IMMEDIATE")
                        try:
                            finalize_db_after_test(
                                connw,
                                links_cols=links_cols,
                                inbound_id=tr.job.inbound_id,
                                link_id=tr.job.link_id,
                                ok=tr.ok,
                                err_code=(tr.err_code if not tr.ok else ""),
                                now=utc_now(),
                                meta=tr.meta,
                            )
                            connw.commit()
                        except Exception as e:
                            connw.rollback()
                            tr.ok = False
                            tr.err_code = "db"
                            tr.err_detail = str(e)

                        if tr.ok:
                            meta = tr.meta or {}
                            ok_items.append(
                                {
                                    "idx": tr.job.idx,
                                    "link_id": tr.job.link_id,
                                    "outbound_tag": tr.job.outbound_tag,
                                    "port": tr.job.port,
                                    "ip": meta.get("ip"),
                                    "country": meta.get("country"),
                                    "city": meta.get("city"),
                                    "datacenter": meta.get("datacenter"),
                                    "duration_sec": round(tr.duration_sec, 3),
                                }
                            )
                            logger.log(
                                f"OK  #{tr.job.idx}/{len(jobs)} link_id={tr.job.link_id} outbound={tr.job.outbound_tag} "
                                f"ip={meta.get('ip')} city={meta.get('city')} dur={tr.duration_sec:.2f}s"
                            )
                        else:
                            fail_items.append(
                                {
                                    "idx": tr.job.idx,
                                    "link_id": tr.job.link_id,
                                    "outbound_tag": tr.job.outbound_tag,
                                    "port": tr.job.port,
                                    "error": _one_word(tr.err_code),
                                    "error_detail": clamp_text(tr.err_detail, 200),
                                    "duration_sec": round(tr.duration_sec, 3),
                                }
                            )
                            logger.log(
                                f"FAIL #{tr.job.idx}/{len(jobs)} link_id={tr.job.link_id} outbound={tr.job.outbound_tag} "
                                f"code={_one_word(tr.err_code)} detail={clamp_text(tr.err_detail, 180)} dur={tr.duration_sec:.2f}s"
                            )

                        while len(in_flight) < PARALLEL_JOBS and submit_next(ex):
                            pass

                        if completed % 10 == 0 or completed == len(jobs):
                            logger.log(f"progress completed={completed}/{len(jobs)} ok={len(ok_items)} fail={len(fail_items)}")

                        break

        finally:
            try:
                connw.close()
            except Exception:
                pass

        finished_at = utc_now()
        duration_sec = (finished_at - started_at).total_seconds()

        report = {
            "status": "ok",
            "batch_id": batch_id,
            "count": count,
            "parallel": PARALLEL_JOBS,
            "ports": {"start": test_port_start, "end": test_port_start + count - 1},
            "db": db_path,
            "started_at": iso_utc(started_at),
            "finished_at": iso_utc(finished_at),
            "duration_sec": round(duration_sec, 3),
            "ok_count": len(ok_items),
            "fail_count": len(fail_items),
            "ok_items": ok_items,
            "fail_items": fail_items,
            "log_file": str(log_path),
            "report_file": str(report_path),
        }

        try:
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as e:
            logger.log(f"ERROR writing report file: {e}")

        logger.log(f"done ok={len(ok_items)} fail={len(fail_items)} duration={duration_sec:.2f}s report={report_path}")
        print(json.dumps(report, ensure_ascii=False, indent=2))
        logger.flush()

    except Exception as e:
        conn3 = connect_db(db_path)
        try:
            conn3.execute("BEGIN IMMEDIATE")
            release_batch_locks(conn3, batch_id=batch_id, ports=ports)
            conn3.commit()
        except Exception:
            conn3.rollback()
        finally:
            try:
                conn3.close()
            except Exception:
                pass

        logger.log(f"ERROR batch failed: {e}")
        logger.flush()
        raise

    finally:
        try:
            if rule_tags:
                xr.remove_rules(rule_tags)
        except Exception:
            pass
        try:
            for t in created_in:
                xr.remove_inbound(t)
        except Exception:
            pass
        try:
            for t in created_out:
                xr.remove_outbound(t)
        except Exception:
            pass

        logger.log("cleanup completed")
        logger.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
