# app/xraymgr/xray_runtime.py
import json
import os
import subprocess
import tempfile
import threading
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


@dataclass(frozen=True)
class CmdResult:
    rc: int
    stdout: str
    stderr: str

    @property
    def ok(self) -> bool:
        return self.rc == 0


class XrayRuntimeApplier:
    """
    Runtime Applier برای Xray (بدون API/وب و بدون DB)

    ورودی این کلاس یک Snapshot کامل است:
      - inbounds:  لیست inbound JSON ها (هر کدام باید tag داشته باشد)
      - outbounds: لیست outbound JSON ها (هر کدام باید tag داشته باشد)
      - routing:   کل routing JSON (به‌صورت کامل؛ با adrules جایگزین می‌شود)

    نکات مهم:
      - tagها را تغییر نمی‌دهیم؛ فقط همان JSON اعمال می‌شود.
      - outbounds/inbounds ناچاراً تکی add/remove می‌شوند (ado/adi + rmo/rmi).
      - routing همیشه Full-Replace است (adrules) و باید کامل از بیرون داده شود.
      - این ماژول هیچ restart انجام نمی‌دهد (restartlogger و ... را استفاده نمی‌کند).
    """

    def __init__(
        self,
        xray_bin: str,
        api_server: str,
        *,
        env: Optional[Dict[str, str]] = None,
        exist_retry: bool = True,
        command_timeout_sec: float = 20.0,
    ) -> None:
        self.xray_bin = xray_bin
        self.api_server = api_server
        self.exist_retry = bool(exist_retry)
        self.command_timeout_sec = float(command_timeout_sec)
        self._lock = threading.Lock()
        self._env = dict(os.environ)
        if env:
            self._env.update({str(k): str(v) for k, v in env.items()})

    # -----------------------------
    # Public: apply snapshot
    # -----------------------------

    def apply_snapshot(self, snapshot: Dict[str, Any]) -> Dict[str, Any]:
        """
        Snapshot schema:
          {
            "inbounds":  [ {...}, {...} ],
            "outbounds": [ {...}, {...} ],
            "routing":   { ... }   # full routing object (مثلاً {"routing": {...}} یا {...} طبق نیاز شما)
          }

        خروجی:
          {
            "ok": bool,
            "error_code": str|None,
            "error_message": str|None,
            "outbounds": [ per-item results ... ],
            "inbounds":  [ per-item results ... ],
            "routing":   result|None
          }
        """
        with self._lock:
            return self._apply_snapshot_locked(snapshot)

    # -----------------------------
    # Internal: helpers
    # -----------------------------

    def _apply_snapshot_locked(self, snapshot: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(snapshot, dict):
            return self._err("INVALID_SNAPSHOT", "snapshot must be a dict")

        if not os.path.exists(self.xray_bin):
            return self._err("XRAY_BIN_NOT_FOUND", f"xray_bin not found: {self.xray_bin}")

        inbounds = snapshot.get("inbounds", [])
        outbounds = snapshot.get("outbounds", [])
        routing = snapshot.get("routing", None)

        if inbounds is None:
            inbounds = []
        if outbounds is None:
            outbounds = []

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
                }

        # 3) apply routing (full replace)
        routing_result: Optional[Dict[str, Any]] = None
        if routing is not None:
            if not isinstance(routing, dict):
                return {
                    "ok": False,
                    "error_code": "INVALID_SNAPSHOT",
                    "error_message": "routing must be dict when provided",
                    "outbounds": results_out,
                    "inbounds": results_in,
                    "routing": None,
                }

            rr = self._apply_routing(routing)
            routing_result = rr
            if not rr.get("ok"):
                return {
                    "ok": False,
                    "error_code": "APPLY_ROUTING_FAILED",
                    "error_message": "failed applying routing",
                    "outbounds": results_out,
                    "inbounds": results_in,
                    "routing": routing_result,
                }

        return {
            "ok": True,
            "error_code": None,
            "error_message": None,
            "outbounds": results_out,
            "inbounds": results_in,
            "routing": routing_result,
        }

    def _run_xray_api_with_config(self, subcommand: str, config_path: str) -> CmdResult:
        cmd = [
            self.xray_bin,
            "api",
            subcommand,
            f"--server={self.api_server}",
            config_path,
        ]
        try:
            p = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                env=self._env,
                timeout=self.command_timeout_sec,
            )
            return CmdResult(p.returncode, (p.stdout or "").strip(), (p.stderr or "").strip())
        except subprocess.TimeoutExpired as e:
            out = ""
            err = f"timeout running command: {cmd!r}"
            if getattr(e, "stdout", None):
                out = str(e.stdout)
            if getattr(e, "stderr", None):
                err = str(e.stderr)
            return CmdResult(124, out, err)
        except Exception as e:
            return CmdResult(1, "", f"failed running command: {e}")

    def _run_xray_api_with_tag(self, subcommand: str, tag: str) -> CmdResult:
        cmd = [
            self.xray_bin,
            "api",
            subcommand,
            f"--server={self.api_server}",
            tag,
        ]
        try:
            p = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                env=self._env,
                timeout=self.command_timeout_sec,
            )
            return CmdResult(p.returncode, (p.stdout or "").strip(), (p.stderr or "").strip())
        except subprocess.TimeoutExpired:
            return CmdResult(124, "", f"timeout running command: {cmd!r}")
        except Exception as e:
            return CmdResult(1, "", f"failed running command: {e}")

    def _stderr_has_exist(self, stderr: str) -> bool:
        s = (stderr or "").lower()
        return "exist" in s or "exists" in s or "already" in s

    def _try_add_outbound(self, outbound: Dict[str, Any]) -> Dict[str, Any]:
        tag = str(outbound.get("tag") or "")
        payload = {"outbounds": [outbound]}
        rc1, out1, err1 = self._run_with_temp_json("ado", payload)

        removed_existing = False
        rm_result: Optional[CmdResult] = None

        if rc1 != 0 and self.exist_retry and self._stderr_has_exist(err1):
            rm = self._run_xray_api_with_tag("rmo", tag)
            rm_result = rm
            removed_existing = rm.ok
            rc2, out2, err2 = self._run_with_temp_json("ado", payload)
            ok = (rc2 == 0)
            return {
                "ok": ok,
                "tag": tag,
                "action": "ado",
                "attempts": 2,
                "rc": rc2,
                "stdout": out2,
                "stderr": err2,
                "removed_existing": removed_existing,
                "remove_rc": rm.rc if rm_result else None,
                "remove_stdout": rm_result.stdout if rm_result else None,
                "remove_stderr": rm_result.stderr if rm_result else None,
            }

        return {
            "ok": (rc1 == 0),
            "tag": tag,
            "action": "ado",
            "attempts": 1,
            "rc": rc1,
            "stdout": out1,
            "stderr": err1,
            "removed_existing": False,
            "remove_rc": None,
            "remove_stdout": None,
            "remove_stderr": None,
        }

    def _try_add_inbound(self, inbound: Dict[str, Any]) -> Dict[str, Any]:
        tag = str(inbound.get("tag") or "")
        payload = {"inbounds": [inbound]}
        rc1, out1, err1 = self._run_with_temp_json("adi", payload)

        removed_existing = False
        rm_result: Optional[CmdResult] = None

        if rc1 != 0 and self.exist_retry and self._stderr_has_exist(err1):
            rm = self._run_xray_api_with_tag("rmi", tag)
            rm_result = rm
            removed_existing = rm.ok
            rc2, out2, err2 = self._run_with_temp_json("adi", payload)
            ok = (rc2 == 0)
            return {
                "ok": ok,
                "tag": tag,
                "action": "adi",
                "attempts": 2,
                "rc": rc2,
                "stdout": out2,
                "stderr": err2,
                "removed_existing": removed_existing,
                "remove_rc": rm.rc if rm_result else None,
                "remove_stdout": rm_result.stdout if rm_result else None,
                "remove_stderr": rm_result.stderr if rm_result else None,
            }

        return {
            "ok": (rc1 == 0),
            "tag": tag,
            "action": "adi",
            "attempts": 1,
            "rc": rc1,
            "stdout": out1,
            "stderr": err1,
            "removed_existing": False,
            "remove_rc": None,
            "remove_stdout": None,
            "remove_stderr": None,
        }

    def _apply_routing(self, routing_obj: Dict[str, Any]) -> Dict[str, Any]:
        rc, out, err = self._run_with_temp_json("adrules", routing_obj)
        return {
            "ok": (rc == 0),
            "action": "adrules",
            "rc": rc,
            "stdout": out,
            "stderr": err,
        }

    def _run_with_temp_json(self, subcommand: str, obj: Dict[str, Any]) -> Tuple[int, str, str]:
        path = None
        try:
            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8") as f:
                json.dump(obj, f, ensure_ascii=False)
                path = f.name
            r = self._run_xray_api_with_config(subcommand, path)
            return r.rc, r.stdout, r.stderr
        finally:
            if path and os.path.exists(path):
                try:
                    os.remove(path)
                except Exception:
                    pass

    def _err(self, code: str, msg: str) -> Dict[str, Any]:
        return {
            "ok": False,
            "error_code": str(code),
            "error_message": str(msg),
            "outbounds": [],
            "inbounds": [],
            "routing": None,
        }
