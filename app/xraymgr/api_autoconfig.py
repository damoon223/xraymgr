from __future__ import annotations

import json
import os
import re
import shutil
import socket
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _run(cmd: List[str], timeout_sec: float = 2.0) -> Tuple[int, str, str]:
    try:
        p = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout_sec,
        )
        return int(p.returncode), p.stdout or "", p.stderr or ""
    except Exception as e:
        return 127, "", str(e)


def _tcp_ok(host: str, port: int, timeout_sec: float = 0.25) -> bool:
    try:
        with socket.create_connection((host, int(port)), timeout=float(timeout_sec)):
            return True
    except Exception:
        return False


def _read_proc_exe(pid: int) -> Optional[str]:
    try:
        return os.readlink(f"/proc/{pid}/exe")
    except Exception:
        return None


def _iter_pids() -> List[int]:
    p = Path("/proc")
    if not p.exists():
        return []
    out: List[int] = []
    for d in p.iterdir():
        if d.is_dir() and d.name.isdigit():
            try:
                out.append(int(d.name))
            except Exception:
                pass
    return out


def _read_cmdline(pid: int) -> List[str]:
    try:
        raw = (Path("/proc") / str(pid) / "cmdline").read_bytes()
        parts = raw.split(b"\x00")
        out: List[str] = []
        for b in parts:
            s = b.decode("utf-8", errors="replace").strip()
            if s:
                out.append(s)
        return out
    except Exception:
        return []


def _read_comm(pid: int) -> str:
    try:
        return (Path("/proc") / str(pid) / "comm").read_text(encoding="utf-8", errors="replace").strip()
    except Exception:
        return ""


def _looks_like_xray(comm: str, cmdline: List[str]) -> bool:
    hay = (" ".join([comm] + cmdline)).lower()
    return ("xray" in hay) or ("x-ui" in hay) or ("3x-ui" in hay)


def _extract_config_path(cmdline: List[str]) -> Optional[str]:
    # Common forms: -c /path/config.json | --config /path/config.json | --config=/path/config.json
    for i, tok in enumerate(cmdline):
        if tok in ("-c", "--config", "-config") and i + 1 < len(cmdline):
            return cmdline[i + 1].strip()
        if tok.startswith("--config="):
            return tok.split("=", 1)[1].strip()
        if tok.startswith("-config="):
            return tok.split("=", 1)[1].strip()
        if tok.startswith("-c="):
            return tok.split("=", 1)[1].strip()
    return None


def _parse_host_port(s: str) -> Optional[Tuple[str, int]]:
    s = (s or "").strip()
    if not s or ":" not in s:
        return None
    if s.startswith("[") and "]" in s:
        host = s[1 : s.index("]")]
        rest = s[s.index("]") + 1 :]
        if rest.startswith(":") and rest[1:].isdigit():
            return host, int(rest[1:])
        return None
    host, ps = s.rsplit(":", 1)
    host = host.strip()
    ps = ps.strip()
    if not ps.isdigit():
        return None
    return host, int(ps)


def _load_json(path: str) -> Optional[Dict[str, Any]]:
    try:
        p = Path(path).expanduser()
        if not p.exists():
            return None
        obj = json.loads(p.read_text(encoding="utf-8", errors="replace") or "{}")
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _extract_api_addr_from_config(cfg: Dict[str, Any]) -> Optional[str]:
    api = cfg.get("api")
    if not isinstance(api, dict):
        return None

    # Preferred: api.listen = "127.0.0.1:10085"
    listen = api.get("listen")
    if isinstance(listen, str) and _parse_host_port(listen):
        return listen.strip()

    # Fallback: find inbound with tag == api.tag (default "api")
    api_tag = str(api.get("tag") or "api").strip()
    inbounds = cfg.get("inbounds")
    if isinstance(inbounds, list):
        for ib in inbounds:
            if not isinstance(ib, dict):
                continue
            if str(ib.get("tag") or "").strip() != api_tag:
                continue
            host = str(ib.get("listen") or "127.0.0.1").strip() or "127.0.0.1"
            port = ib.get("port")
            if isinstance(port, int) and port > 0:
                return f"{host}:{port}"
            if isinstance(port, str) and port.isdigit():
                return f"{host}:{int(port)}"

    return None


def _default_xray_bin() -> Optional[str]:
    # Prefer x-ui default path first, then common locations, then PATH
    candidates = (
        os.environ.get("XRAYMGR_XRAY_BIN", "").strip(),
        "/usr/local/x-ui/bin/xray-linux-amd64",
        "/usr/local/bin/xray",
        "/usr/bin/xray",
        "xray",
    )
    for c in candidates:
        if not c:
            continue
        if os.path.sep not in c:
            resolved = shutil.which(c)
            if resolved:
                return resolved
            continue
        if os.path.exists(c) and os.access(c, os.X_OK):
            return c
    return None


def _probe_grpc_api_with_xray_cli(xray_bin: str, api_server: str) -> bool:
    # Use a cheap API call that returns JSON when gRPC API is reachable.
    # xray api lso --server=127.0.0.1:PORT
    rc, out, err = _run([xray_bin, "api", "lso", f"--server={api_server}"], timeout_sec=2.5)
    if rc != 0:
        return False
    try:
        js = json.loads((out or "").strip() or "null")
        return isinstance(js, (list, dict))
    except Exception:
        return False


def _detect_from_running_xray_config() -> Dict[str, Any]:
    for pid in _iter_pids():
        comm = _read_comm(pid)
        cmdline = _read_cmdline(pid)
        if not _looks_like_xray(comm, cmdline):
            continue

        cfg_path = _extract_config_path(cmdline)
        exe = _read_proc_exe(pid)
        det: Dict[str, Any] = {"method": "proc_config", "pid": pid, "comm": comm, "config_path": cfg_path, "exe": exe}

        if not cfg_path:
            continue
        cfg = _load_json(cfg_path)
        if not isinstance(cfg, dict):
            continue

        api_addr = _extract_api_addr_from_config(cfg)
        if not api_addr:
            continue

        hp = _parse_host_port(api_addr)
        if not hp:
            continue

        host, port = hp
        connect_host = "127.0.0.1" if host in ("0.0.0.0", "") else host
        api_server = f"{connect_host}:{port}"

        xray_bin = exe if (exe and os.path.exists(exe) and os.access(exe, os.X_OK)) else _default_xray_bin()
        cli_ok = bool(xray_bin) and _probe_grpc_api_with_xray_cli(str(xray_bin), api_server)
        tcp_ok = _tcp_ok(connect_host, port)

        det.update({"api_server": api_server, "xray_bin": xray_bin, "cli_ok": cli_ok, "tcp_ok": tcp_ok})
        if cli_ok or tcp_ok:
            det["ok"] = True
            return det

    return {"ok": False, "method": "proc_config", "error": "no_api_addr_detected"}


def _parse_ss_xray_listeners() -> List[Dict[str, Any]]:
    rc, out, err = _run(["ss", "-lntp"], timeout_sec=2.5)
    text = (out or "") + "\n" + (err or "")
    if rc != 0 and not text.strip():
        return []

    items: List[Dict[str, Any]] = []
    for line in text.splitlines():
        if "LISTEN" not in line.upper():
            continue
        if "xray" not in line.lower():
            continue

        # IPv4 local addr:port
        m4 = re.search(r"\s(\d{1,3}(?:\.\d{1,3}){3}):(\d+)\s", line)
        # IPv6 local addr:port (best-effort)
        m6 = re.search(r"\s\[(.+?)\]:(\d+)\s", line)

        ip = None
        port = None
        if m4:
            ip = m4.group(1)
            port = int(m4.group(2))
        elif m6:
            ip = m6.group(1)
            port = int(m6.group(2))

        pid = None
        mp = re.search(r"pid=(\d+)", line)
        if mp:
            try:
                pid = int(mp.group(1))
            except Exception:
                pid = None

        if ip and port:
            items.append({"ip": ip, "port": port, "pid": pid, "raw": line.strip()})

    return items


def _detect_from_ss() -> Dict[str, Any]:
    items = _parse_ss_xray_listeners()
    if not items:
        return {"ok": False, "method": "ss", "error": "no_xray_listeners_found"}

    # Exclude common metrics port and prioritize 127.0.0.1
    filtered = [i for i in items if int(i["port"]) != 11111]
    candidates = [i for i in filtered if str(i["ip"]) == "127.0.0.1"] or filtered or items

    # Try to validate via xray CLI using the owning PID's exe, then fallback to TCP-connect
    for it in candidates:
        host = "127.0.0.1" if str(it["ip"]) in ("0.0.0.0", "") else str(it["ip"])
        port = int(it["port"])
        api_server = f"{host}:{port}"

        xray_bin = None
        if it.get("pid"):
            exe = _read_proc_exe(int(it["pid"]))
            if exe and os.path.exists(exe) and os.access(exe, os.X_OK):
                xray_bin = exe
        if not xray_bin:
            xray_bin = _default_xray_bin()

        cli_ok = bool(xray_bin) and _probe_grpc_api_with_xray_cli(str(xray_bin), api_server)
        tcp_ok = _tcp_ok(host, port)

        if cli_ok or tcp_ok:
            return {
                "ok": True,
                "method": "ss",
                "api_server": api_server,
                "xray_bin": xray_bin,
                "cli_ok": cli_ok,
                "tcp_ok": tcp_ok,
                "picked": it,
                "candidates": candidates[:10],
            }

    # Best-effort: pick first candidate even if validation failed
    it = candidates[0]
    host = "127.0.0.1" if str(it["ip"]) in ("0.0.0.0", "") else str(it["ip"])
    port = int(it["port"])
    return {"ok": True, "method": "ss", "api_server": f"{host}:{port}", "picked": it, "candidates": candidates[:10], "tcp_ok": False, "cli_ok": False}


def detect_xray_api_server() -> Dict[str, Any]:
    # 1) Best signal: running xray config -> api.listen or inbound(tag==api.tag)
    d1 = _detect_from_running_xray_config()
    if d1.get("ok") and d1.get("api_server"):
        return d1

    # 2) Fallback: parse `ss -lntp` output and validate candidates
    d2 = _detect_from_ss()
    if d2.get("ok") and d2.get("api_server"):
        d2["fallback_from"] = d1
        return d2

    return {"ok": False, "error": "DETECT_FAILED", "details": {"proc_config": d1, "ss": d2}}


def _atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8", dir=str(path.parent)) as tf:
        tf.write(text)
        tmp = Path(tf.name)
    tmp.replace(path)


def _update_py_constant(text: str, name: str, value_literal: str) -> Tuple[str, bool]:
    # Replace: NAME = ...
    pat = re.compile(rf"(?m)^\s*{re.escape(name)}\s*=\s*.*?$")
    repl = f'{name} = {value_literal}'
    if pat.search(text):
        return pat.sub(repl, text), True

    # Append if missing
    sep = "" if (text.endswith("\n") or not text) else "\n"
    return f"{text}{sep}{repl}\n", True


def write_api_server_to_test_settings(api_server: str) -> Dict[str, Any]:
    # Writes into: app/xraymgr/test_settings.py  (same directory as this file)
    target = Path(__file__).with_name("test_settings.py").resolve()
    current = ""
    if target.exists():
        current = target.read_text(encoding="utf-8", errors="replace")

    new_text, changed = _update_py_constant(current, "TEST_API_SERVER", json.dumps(str(api_server).strip()))
    if changed:
        _atomic_write_text(target, new_text)

    return {"ok": True, "path": str(target), "api_server": str(api_server).strip(), "changed": bool(changed)}


def autodetect_and_write() -> Dict[str, Any]:
    det = detect_xray_api_server()
    if not det.get("ok") or not det.get("api_server"):
        return {"ok": False, "error": "DETECT_FAILED", "detect": det}
    wr = write_api_server_to_test_settings(str(det["api_server"]))
    return {"ok": True, "detect": det, "write": wr}


def main() -> None:
    res = autodetect_and_write()
    print(json.dumps(res, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
