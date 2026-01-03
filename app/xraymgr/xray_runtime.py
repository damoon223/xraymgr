# app/xraymgr/xray_runtime.py
"""
Xray runtime control (CLI-based) for x-ui managed deployments.

Key facts from on-server validation (x-ui installation):
- Xray binary may be located at /usr/local/x-ui/bin/xray-linux-amd64
- Xray provides a CLI "xray api <cmd>" which talks to an in-process gRPC API server
  configured in the running Xray config (e.g., 127.0.0.1:10085).
- Supported CLI commands used here: lsi/lso, adi/ado, rmi/rmo, adrules/rmrules, stats, etc.

This module is intentionally independent from:
- web.py / FastAPI
- the SQLite DB
- any x-ui panel automation

It provides:
- Listing inbounds/outbounds (lsi/lso) so callers can introspect runtime state.
- Add/remove inbounds/outbounds (adi/ado + rmi/rmo).
- A snapshot applier that can (optionally) remove selected tags AFTER routing has been applied,
  to support safe cutover semantics ("add -> route -> remove old").
"""

from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import tempfile
import threading
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


DEFAULT_XRAY_BIN_CANDIDATES: Tuple[str, ...] = (
    # x-ui default
    "/usr/local/x-ui/bin/xray-linux-amd64",
    # common locations
    "/usr/local/bin/xray",
    "/usr/bin/xray",
    "xray",
)

DEFAULT_API_SERVER_CANDIDATES: Tuple[str, ...] = (
    "127.0.0.1:10085",  # common for x-ui/3x-ui
    "127.0.0.1:8080",   # historical default in examples
    "127.0.0.1:11111",  # sometimes present; may not be API though
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
        # "xray" in PATH
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


def _stderr_has_exist(stderr: str) -> bool:
    s = (stderr or "").lower()
    # heuristics: different builds print different phrases
    return ("exist" in s) or ("already" in s) or ("duplicate" in s)


class XrayRuntimeApplier:
    """
    Runtime Applier برای Xray (CLI-based) — بدون API/وب و بدون DB.

    ورودی apply_snapshot یک Snapshot کامل است:
      - inbounds:  لیست inbound JSON ها (هر کدام باید tag داشته باشد)
      - outbounds: لیست outbound JSON ها (هر کدام باید tag داشته باشد)
      - routing:   object مربوط به routing (اگر داده شود اعمال می‌شود)

    نکات:
      - tagها را تغییر نمی‌دهیم؛ فقط همان JSON اعمال می‌شود.
      - outbounds/inbounds با ado/adi اضافه می‌شوند.
      - اگر tag موجود باشد و exist_retry=True، ابتدا rmo/rmi اجرا می‌شود و سپس دوباره add.
      - حذف (remove) اختیاری است و اگر در snapshot آمده باشد، بعد از routing انجام می‌شود
        تا cutover امن‌تر باشد (add -> route -> remove old).
    """

    def __init__(
        self,
        xray_bin: Optional[str] = None,
        api_server: str = "auto",
        *,
        env: Optional[Dict[str, str]] = None,
        exist_retry: bool = True,
        command_timeout_sec: float = 20.0,
        api_probe_timeout_sec: float = 3.0,
    ) -> None:
        self.xray_bin = xray_bin or _first_existing_executable(DEFAULT_XRAY_BIN_CANDIDATES) or str(xray_bin or "")
        self.api_server = str(api_server)
        self.exist_retry = bool(exist_retry)
        self.command_timeout_sec = float(command_timeout_sec)
        self.api_probe_timeout_sec = float(api_probe_timeout_sec)

        self._lock = threading.Lock()
        self._env = dict(os.environ)
        if env:
            self._env.update({str(k): str(v) for k, v in env.items()})

        if self.api_server.strip().lower() == "auto":
            self.api_server = self.probe_api_server()

    # -----------------------------
    # Public: probes / listing
    # -----------------------------

    def probe_api_server(self, candidates: Sequence[str] = DEFAULT_API_SERVER_CANDIDATES) -> str:
        """
        Find a working API server endpoint by trying `lso` with short timeouts.
        Falls back to first candidate if none responds successfully.
        """
        if not self.xray_bin:
            return str(candidates[0])

        if not os.path.exists(self.xray_bin) and os.path.sep in self.xray_bin:
            return str(candidates[0])

        for srv in candidates:
            r = self._run_xray_api("lso", server=srv, timeout_sec=self.api_probe_timeout_sec)
            if r.ok and _try_parse_json(r.stdout) is not None:
                return str(srv)

        return str(candidates[0])

    def list_outbounds(self) -> Dict[str, Any]:
        with self._lock:
            return self._list_outbounds_locked()

    def list_inbounds(self) -> Dict[str, Any]:
        with self._lock:
            return self._list_inbounds_locked()

    def list_outbound_tags(self) -> List[str]:
        r = self.list_outbounds()
        if not r.get("ok"):
            return []
        out = r.get("data") or {}
        tags: List[str] = []
        for ob in (out.get("outbounds") or []):
            if isinstance(ob, dict) and ob.get("tag"):
                tags.append(str(ob["tag"]))
        return tags

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

    # -----------------------------
    # Public: primitives
    # -----------------------------

    def add_outbound(self, outbound: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock:
            return self._try_add_outbound(outbound)

    def remove_outbound(self, tag: str, *, ignore_not_found: bool = True) -> Dict[str, Any]:
        with self._lock:
            r = self._run_xray_api("rmo", args=[str(tag)])
            ok = r.ok or (ignore_not_found and (_looks_like_not_found(r.stdout) or _looks_like_not_found(r.stderr)))
            return {"ok": ok, "tag": str(tag), "action": "rmo", "rc": r.rc, "stdout": r.stdout, "stderr": r.stderr}

    def add_inbound(self, inbound: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock:
            return self._try_add_inbound(inbound)

    def remove_inbound(self, tag: str, *, ignore_not_found: bool = True) -> Dict[str, Any]:
        with self._lock:
            r = self._run_xray_api("rmi", args=[str(tag)])
            ok = r.ok or (ignore_not_found and (_looks_like_not_found(r.stdout) or _looks_like_not_found(r.stderr)))
            return {"ok": ok, "tag": str(tag), "action": "rmi", "rc": r.rc, "stdout": r.stdout, "stderr": r.stderr}

    def remove_rules(self, rule_tags: Iterable[str], *, ignore_not_found: bool = True) -> Dict[str, Any]:
        """
        Remove routing rules by ruleTag (rmrules).
        """
        tags = [str(t) for t in rule_tags if str(t).strip()]
        if not tags:
            return {"ok": True, "action": "rmrules", "removed": [], "errors": []}

        with self._lock:
            removed: List[str] = []
            errors: List[Dict[str, Any]] = []
            for t in tags:
                r = self._run_xray_api("rmrules", args=[t])
                ok = r.ok or (ignore_not_found and (_looks_like_not_found(r.stdout) or _looks_like_not_found(r.stderr)))
                if ok:
                    removed.append(t)
                else:
                    errors.append({"tag": t, "rc": r.rc, "stdout": r.stdout, "stderr": r.stderr})
            return {"ok": len(errors) == 0, "action": "rmrules", "removed": removed, "errors": errors}

    def apply_rules(self, routing_obj: Dict[str, Any], *, append: bool = False) -> Dict[str, Any]:
        """
        Apply routing rules via `adrules`.

        Note:
          - If append=False (default), CLI semantics replace existing routing configuration.
          - If append=True, rules are appended.
        """
        with self._lock:
            payload = self._normalize_routing_payload(routing_obj)
            args = ["-append"] if append else []
            r = self._run_with_temp_json("adrules", payload, extra_args=args)
            return {"ok": r.ok, "action": "adrules", "append": bool(append), "rc": r.rc, "stdout": r.stdout, "stderr": r.stderr}

    # -----------------------------
    # Public: apply snapshot
    # -----------------------------

    def apply_snapshot(self, snapshot: Dict[str, Any]) -> Dict[str, Any]:
        """
        Snapshot schema:
          {
            "inbounds":  [ {...}, {...} ],
            "outbounds": [ {...}, {...} ],
            "routing":   { ... },                # optional
            "remove_outbound_tags": [ ... ],     # optional; executed AFTER routing
            "remove_inbound_tags":  [ ... ],     # optional; executed AFTER routing
            "remove_rule_tags":     [ ... ],     # optional; executed BEFORE routing (cleanup)
            "routing_append": false|true         # optional (default: false)
          }

        خروجی:
          {
            "ok": bool,
            "error_code": str|None,
            "error_message": str|None,
            "outbounds": [ per-item results ... ],
            "inbounds":  [ per-item results ... ],
            "routing":   result|None,
            "removals":  { ... }|None
          }
        """
        with self._lock:
            return self._apply_snapshot_locked(snapshot)

    # -----------------------------
    # Internal
    # -----------------------------

    def _require_ready(self) -> Optional[Dict[str, Any]]:
        if not self.xray_bin:
            return self._err("XRAY_BIN_NOT_SET", "xray_bin is empty")
        if os.path.sep in self.xray_bin and not os.path.exists(self.xray_bin):
            return self._err("XRAY_BIN_NOT_FOUND", f"xray_bin not found: {self.xray_bin}")
        return None

    def _list_outbounds_locked(self) -> Dict[str, Any]:
        e = self._require_ready()
        if e:
            return e
        r = self._run_xray_api("lso")
        data = _try_parse_json(r.stdout)
        return {"ok": r.ok and data is not None, "action": "lso", "rc": r.rc, "stdout": r.stdout, "stderr": r.stderr, "data": data}

    def _list_inbounds_locked(self) -> Dict[str, Any]:
        e = self._require_ready()
        if e:
            return e
        r = self._run_xray_api("lsi")
        data = _try_parse_json(r.stdout)
        return {"ok": r.ok and data is not None, "action": "lsi", "rc": r.rc, "stdout": r.stdout, "stderr": r.stderr, "data": data}

    def _apply_snapshot_locked(self, snapshot: Dict[str, Any]) -> Dict[str, Any]:
        e = self._require_ready()
        if e:
            return e

        if not isinstance(snapshot, dict):
            return self._err("INVALID_SNAPSHOT", "snapshot must be a dict")

        inbounds = snapshot.get("inbounds", []) or []
        outbounds = snapshot.get("outbounds", []) or []
        routing = snapshot.get("routing", None)

        remove_outbound_tags = snapshot.get("remove_outbound_tags", []) or []
        remove_inbound_tags = snapshot.get("remove_inbound_tags", []) or []
        remove_rule_tags = snapshot.get("remove_rule_tags", []) or []
        routing_append = bool(snapshot.get("routing_append", False))

        if not isinstance(inbounds, list) or not isinstance(outbounds, list):
            return self._err("INVALID_SNAPSHOT", "inbounds/outbounds must be lists")

        # validate tags presence (بدون تغییر tag)
        for i, ob in enumerate(outbounds):
            if not isinstance(ob, dict):
                return self._err("INVALID_SNAPSHOT", f"outbounds[{i}] must be dict")
            if not ob.get("tag"):
                return self._err("INVALID_SNAPSHOT", f"outbounds[{i}] missing 'tag'")

        for i, ib in enumerate(inbounds):
            if not isinstance(ib, dict):
                return self._err("INVALID_SNAPSHOT", f"inbounds[{i}] must be dict")
            if not ib.get("tag"):
                return self._err("INVALID_SNAPSHOT", f"inbounds[{i}] missing 'tag'")

        results_out: List[Dict[str, Any]] = []
        results_in: List[Dict[str, Any]] = []

        # 1) add/replace outbounds
        for ob in outbounds:
            r = self._try_add_outbound(ob)
            results_out.append(r)
            if not r.get("ok"):
                return {
                    "ok": False,
                    "error_code": "APPLY_OUTBOUNDS_FAILED",
                    "error_message": "failed applying outbounds; see item results",
                    "outbounds": results_out,
                    "inbounds": results_in,
                    "routing": None,
                    "removals": None,
                }

        # 2) add/replace inbounds
        for ib in inbounds:
            r = self._try_add_inbound(ib)
            results_in.append(r)
            if not r.get("ok"):
                return {
                    "ok": False,
                    "error_code": "APPLY_INBOUNDS_FAILED",
                    "error_message": "failed applying inbounds; see item results",
                    "outbounds": results_out,
                    "inbounds": results_in,
                    "routing": None,
                    "removals": None,
                }

        # 3) remove old rules (optional cleanup), then apply routing (optional)
        routing_result: Optional[Dict[str, Any]] = None
        if remove_rule_tags:
            rr = self.remove_rules(remove_rule_tags, ignore_not_found=True)
            if not rr.get("ok"):
                return {
                    "ok": False,
                    "error_code": "REMOVE_RULES_FAILED",
                    "error_message": "failed removing routing rules",
                    "outbounds": results_out,
                    "inbounds": results_in,
                    "routing": None,
                    "removals": {"rules": rr},
                }

        if routing is not None:
            if not isinstance(routing, dict):
                return {
                    "ok": False,
                    "error_code": "INVALID_SNAPSHOT",
                    "error_message": "routing must be dict when provided",
                    "outbounds": results_out,
                    "inbounds": results_in,
                    "routing": None,
                    "removals": None,
                }
            routing_result = self.apply_rules(routing, append=routing_append)
            if not routing_result.get("ok"):
                return {
                    "ok": False,
                    "error_code": "APPLY_ROUTING_FAILED",
                    "error_message": "failed applying routing",
                    "outbounds": results_out,
                    "inbounds": results_in,
                    "routing": routing_result,
                    "removals": None,
                }

        # 4) removals AFTER routing (cutover-safe)
        removals: Dict[str, Any] = {"outbounds": [], "inbounds": []}
        for t in remove_outbound_tags:
            removals["outbounds"].append(self.remove_outbound(str(t), ignore_not_found=True))
        for t in remove_inbound_tags:
            removals["inbounds"].append(self.remove_inbound(str(t), ignore_not_found=True))

        return {
            "ok": True,
            "error_code": None,
            "error_message": None,
            "outbounds": results_out,
            "inbounds": results_in,
            "routing": routing_result,
            "removals": removals if (remove_outbound_tags or remove_inbound_tags) else None,
        }

    def _run_xray_api(
        self,
        subcommand: str,
        *,
        server: Optional[str] = None,
        args: Optional[Sequence[str]] = None,
        timeout_sec: Optional[float] = None,
    ) -> CmdResult:
        srv = str(server or self.api_server)
        cmd: List[str] = [self.xray_bin, "api", str(subcommand), f"--server={srv}"]
        if args:
            cmd.extend([str(a) for a in args])

        try:
            p = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                env=self._env,
                timeout=float(timeout_sec if timeout_sec is not None else self.command_timeout_sec),
            )
            return CmdResult(p.returncode, (p.stdout or "").strip(), (p.stderr or "").strip())
        except subprocess.TimeoutExpired as e:
            out = ""
            err = f"timeout running command: {cmd!r}"
            if getattr(e, "stdout", None):
                out = str(e.stdout)
            if getattr(e, "stderr", None):
                err = str(e.stderr)
            return CmdResult(124, (out or "").strip(), (err or "").strip())
        except Exception as e:
            return CmdResult(1, "", f"failed running command: {e}")

    def _run_with_temp_json(self, subcommand: str, obj: Dict[str, Any], *, extra_args: Optional[Sequence[str]] = None) -> CmdResult:
        path = None
        try:
            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8") as f:
                json.dump(obj, f, ensure_ascii=False)
                path = f.name
            args = list(extra_args or [])
            args.append(path)
            return self._run_xray_api(subcommand, args=args)
        finally:
            if path and os.path.exists(path):
                try:
                    os.remove(path)
                except Exception:
                    pass

    def _try_add_outbound(self, outbound: Dict[str, Any]) -> Dict[str, Any]:
        tag = str(outbound.get("tag") or "")
        payload = {"outbounds": [outbound]}
        r1 = self._run_with_temp_json("ado", payload)

        removed_existing = False
        rm_result: Optional[CmdResult] = None

        if (not r1.ok) and self.exist_retry and _stderr_has_exist(r1.stderr):
            rm = self._run_xray_api("rmo", args=[tag])
            rm_result = rm
            removed_existing = rm.ok
            r2 = self._run_with_temp_json("ado", payload)
            return {
                "ok": r2.ok,
                "tag": tag,
                "action": "ado",
                "attempts": 2,
                "rc": r2.rc,
                "stdout": r2.stdout,
                "stderr": r2.stderr,
                "removed_existing": removed_existing,
                "remove_rc": rm.rc if rm_result else None,
                "remove_stdout": rm_result.stdout if rm_result else None,
                "remove_stderr": rm_result.stderr if rm_result else None,
            }

        return {
            "ok": r1.ok,
            "tag": tag,
            "action": "ado",
            "attempts": 1,
            "rc": r1.rc,
            "stdout": r1.stdout,
            "stderr": r1.stderr,
            "removed_existing": False,
            "remove_rc": None,
            "remove_stdout": None,
            "remove_stderr": None,
        }

    def _try_add_inbound(self, inbound: Dict[str, Any]) -> Dict[str, Any]:
        tag = str(inbound.get("tag") or "")
        payload = {"inbounds": [inbound]}
        r1 = self._run_with_temp_json("adi", payload)

        removed_existing = False
        rm_result: Optional[CmdResult] = None

        if (not r1.ok) and self.exist_retry and _stderr_has_exist(r1.stderr):
            rm = self._run_xray_api("rmi", args=[tag])
            rm_result = rm
            removed_existing = rm.ok
            r2 = self._run_with_temp_json("adi", payload)
            return {
                "ok": r2.ok,
                "tag": tag,
                "action": "adi",
                "attempts": 2,
                "rc": r2.rc,
                "stdout": r2.stdout,
                "stderr": r2.stderr,
                "removed_existing": removed_existing,
                "remove_rc": rm.rc if rm_result else None,
                "remove_stdout": rm_result.stdout if rm_result else None,
                "remove_stderr": rm_result.stderr if rm_result else None,
            }

        return {
            "ok": r1.ok,
            "tag": tag,
            "action": "adi",
            "attempts": 1,
            "rc": r1.rc,
            "stdout": r1.stdout,
            "stderr": r1.stderr,
            "removed_existing": False,
            "remove_rc": None,
            "remove_stdout": None,
            "remove_stderr": None,
        }

    def _normalize_routing_payload(self, routing_obj: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize to what `xray api adrules` expects.

        In practice, people provide either:
          A) {"routing": {...}}  (full block)
          B) {...} where {...} is already routing block (contains "rules"/"domainStrategy"...)

        This normalizer accepts both.
        """
        if "routing" in routing_obj and isinstance(routing_obj.get("routing"), dict):
            return routing_obj
        # heuristics: treat as routing block if it looks like one
        if any(k in routing_obj for k in ("rules", "domainStrategy", "domain_strategy", "balancers", "balancer")):
            return {"routing": routing_obj}
        # fallback: pass-through
        return routing_obj

    def _err(self, code: str, msg: str) -> Dict[str, Any]:
        return {
            "ok": False,
            "error_code": str(code),
            "error_message": str(msg),
            "outbounds": [],
            "inbounds": [],
            "routing": None,
        }


def tcp_probe(host: str, port: int, timeout_sec: float = 1.0) -> bool:
    """
    Lightweight TCP connect probe (does not validate gRPC protocol).
    Useful for checking "is the API port listening?" after restarts.
    """
    try:
        with socket.create_connection((host, int(port)), timeout=float(timeout_sec)):
            return True
    except Exception:
        return False
