import json
import os
import select
import subprocess
import tempfile
import threading
import time
from typing import Any, Dict, Optional, Union


DEFAULT_NODE_TIMEOUT = 15.0          # Convert timeout (seconds)
DEFAULT_READY_TIMEOUT = 20.0         # READY wait (seconds)


class NodeBridgeError(Exception):
    pass


# --- Node persistent bridge (exactly line-based, outputs JSON.stringify(out.toJson()) ) ---
BRIDGE_JS = r"""
'use strict';
const fs = require('fs');
const path = require('path');
const vm = require('vm');
const readline = require('readline');

const BUNDLE_DIR = process.env.WEB_BUNDLE_DIR || process.cwd();

// Polyfills حداقلی، فقط اگر نبودند:
function polyfill() {
  if (typeof global.ObjectUtil === 'undefined') {
    global.ObjectUtil = {
      isEmpty: v => v === undefined || v === null || v === '',
      isArrEmpty: a => !Array.isArray(a) || a.length === 0
    };
  }
  if (typeof global.Base64 === 'undefined') {
    global.Base64 = {
      decode: s => {
        s = String(s).replace(/-/g, '+').replace(/_/g, '/');
        const pad = s.length % 4;
        if (pad === 2) s += '==';
        else if (pad === 3) s += '=';
        else if (pad !== 0) s += '==';
        return Buffer.from(s, 'base64').toString('utf8');
      }
    };
  }
  if (typeof global.atob === 'undefined') {
    global.atob = s => Buffer.from(String(s), 'base64').toString('utf8');
  }
  if (typeof global.Wireguard === 'undefined') {
    global.Wireguard = { generateKeypair: sk => ({ publicKey: '' }) };
  }
  global.data = undefined;
}

function runFile(p) {
  const code = fs.readFileSync(p, 'utf8');
  vm.runInThisContext(code, { filename: p });
}

function runFirstExisting(list) {
  for (const rp of list) {
    const p = path.join(BUNDLE_DIR, rp);
    if (fs.existsSync(p)) { runFile(p); return true; }
  }
  return false;
}

function findAndRunDeep(rootDir, targetName) {
  const stack = [rootDir];
  while (stack.length) {
    const dir = stack.pop();
    let ents;
    try { ents = fs.readdirSync(dir, { withFileTypes: true }); } catch { continue; }
    for (const e of ents) {
      const p = path.join(dir, e.name);
      if (e.isDirectory()) stack.push(p);
      else if (e.isFile() && e.name.toLowerCase() === targetName.toLowerCase()) { runFile(p); return true; }
    }
  }
  return false;
}

polyfill();

// utils (اختیاری)
runFirstExisting(['assets/js/util/common.js','js/util/common.js','util/common.js','common.js']);
runFirstExisting(['assets/js/util/utils.js','js/util/utils.js','util/utils.js','utils.js']);
runFirstExisting(['assets/js/util/date-util.js','js/util/date-util.js','util/date-util.js','date-util.js']);
runFirstExisting(['assets/js/util/wireguard.js','js/util/wireguard.js','util/wireguard.js','wireguard.js']);

// outbound.js (اجباری)
if (!(runFirstExisting(['assets/js/model/outbound.js','js/model/outbound.js','model/outbound.js','outbound.js'])
   || findAndRunDeep(BUNDLE_DIR, 'outbound.js'))) {
  console.error('outbound.js not found in WEB_BUNDLE_DIR');
  process.stdout.write('ERR:OUTBOUND_NOT_FOUND\n');
  process.exit(2);
}

// سیگنال آماده
process.stdout.write('READY\n');

// هر خط ورودی → یک خط خروجی (بدون دستکاری)
const rl = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
rl.on('line', (line) => {
  try {
    line = String(line || '').trim();
    if (!line) { process.stdout.write('null\n'); return; }
    const out = Outbound.fromLink(line);
    const json = out ? (out.toJson ? out.toJson() : out) : null;
    const text = json ? JSON.stringify(json) : 'null';
    process.stdout.write(text + '\n');
  } catch (err) {
    console.error(err && err.stack ? err.stack : String(err));
    process.stdout.write('ERR:EX\n');
  }
});
"""


def _default_webbundle_dir() -> str:
    # مسیر درست روی سرور شما:
    # /opt/xraymgr/app/xraymgr/webbundle/
    env_dir = os.environ.get("XRAYMGR_WEBBUNDLE_DIR") or os.environ.get("WEB_BUNDLE_DIR")
    if env_dir and str(env_dir).strip():
        return os.path.abspath(os.fspath(env_dir))

    here = os.path.dirname(os.path.abspath(__file__))
    cand = os.path.join(here, "webbundle")
    if os.path.isdir(cand):
        return os.path.abspath(cand)

    hard = "/opt/xraymgr/app/xraymgr/webbundle"
    return os.path.abspath(hard)


class NodeLinkConverter:
    """
    معادل Python از JsLinkConverter (C#):
      - یک پردازه‌ی Node پایدار بالا می‌آورد
      - READY را می‌خواند
      - برای هر لینک یک خط خروجی می‌گیرد (JSON/null/ERR)
      - stderr را drain می‌کند تا Node block نشود
      - با timeout، پردازه kill می‌شود و call خروجی None می‌دهد
    """

    def __init__(
        self,
        scripts_dir: Optional[str] = None,
        node_path: Optional[str] = None,
        timeout: float = DEFAULT_NODE_TIMEOUT,
        ready_timeout: float = DEFAULT_READY_TIMEOUT,
    ) -> None:
        self.scripts_dir = os.path.abspath(os.fspath(scripts_dir or _default_webbundle_dir()))
        if not os.path.isdir(self.scripts_dir):
            raise NodeBridgeError(f"WEB_BUNDLE_DIR not found: {self.scripts_dir}")

        self.node_path = node_path or os.environ.get("XRAYMGR_NODE_PATH") or "node"
        self.timeout = float(timeout)
        self.ready_timeout = float(ready_timeout)

        self._proc: Optional[subprocess.Popen] = None
        self._bridge_path: Optional[str] = None
        self._io_lock = threading.Lock()
        self._stderr_thread: Optional[threading.Thread] = None
        self._disposed = False

        self._start_process()

    def _drain_stderr_forever(self) -> None:
        proc = self._proc
        if not proc or not proc.stderr:
            return
        try:
            for _line in proc.stderr:
                pass
        except Exception:
            pass

    def _kill_process_silently(self) -> None:
        proc = self._proc
        self._proc = None

        if proc is not None:
            try:
                if proc.poll() is None:
                    proc.kill()
            except Exception:
                pass
            try:
                proc.wait(timeout=2.0)
            except Exception:
                pass

        if self._bridge_path:
            try:
                os.remove(self._bridge_path)
            except Exception:
                pass
            self._bridge_path = None

        self._stderr_thread = None

    def _wait_ready(self) -> None:
        proc = self._proc
        if proc is None or proc.stdout is None:
            raise NodeBridgeError("Node process is not available.")

        deadline = time.monotonic() + self.ready_timeout
        while True:
            if proc.poll() is not None:
                raise NodeBridgeError("Node bridge exited before READY.")
            now = time.monotonic()
            if now >= deadline:
                raise NodeBridgeError("Node bridge did not signal READY in time.")

            try:
                rlist, _, _ = select.select([proc.stdout], [], [], 0.25)
            except Exception:
                continue
            if not rlist:
                continue

            line = proc.stdout.readline()
            if not line:
                continue

            s = line.strip()
            if s == "READY":
                return
            if s.startswith("ERR:"):
                raise NodeBridgeError(f"Node bridge init failed: {s}")

    def _start_process(self) -> None:
        self._kill_process_silently()

        fd, path = tempfile.mkstemp(prefix="xraymgr_node_bridge_", suffix=".js", text=True)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(BRIDGE_JS)
        self._bridge_path = path

        env = os.environ.copy()
        env["WEB_BUNDLE_DIR"] = self.scripts_dir

        try:
            proc = subprocess.Popen(
                [self.node_path, self._bridge_path],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                bufsize=1,
                universal_newlines=True,
                env=env,
            )
        except OSError as e:
            self._kill_process_silently()
            raise NodeBridgeError(f"Failed to start node process: {e}") from e

        self._proc = proc

        self._stderr_thread = threading.Thread(target=self._drain_stderr_forever, daemon=True)
        self._stderr_thread.start()

        try:
            self._wait_ready()
        except Exception:
            self._kill_process_silently()
            raise

    def convert(self, link: str, timeout: Optional[float] = None) -> Optional[str]:
        """
        خروجی:
          - JSON string (همان چیزی که Node چاپ کرده)
          - یا None اگر null/ERR/timeout/invalid
        """
        if self._disposed:
            raise NodeBridgeError("Converter is disposed.")
        if not link or not str(link).strip():
            return None

        to = float(timeout) if timeout is not None else self.timeout
        normalized = str(link).replace("\r", " ").replace("\n", " ")

        with self._io_lock:
            proc = self._proc
            if proc is None or proc.poll() is not None:
                self._start_process()
                proc = self._proc

            if proc is None or proc.stdin is None or proc.stdout is None:
                return None

            try:
                proc.stdin.write(normalized + "\n")
                proc.stdin.flush()
            except Exception:
                self._kill_process_silently()
                return None

            try:
                rlist, _, _ = select.select([proc.stdout], [], [], to)
            except Exception:
                self._kill_process_silently()
                return None

            if not rlist:
                # timeout => kill like C# (so next call restarts clean)
                self._kill_process_silently()
                return None

            line = proc.stdout.readline()
            if not line:
                self._kill_process_silently()
                return None

            s = line.strip()
            if not s:
                return None
            if s.lower() == "null":
                return None
            if s.startswith("ERR:"):
                return None

            return s

    def close(self) -> None:
        self._disposed = True
        self._kill_process_silently()

    def __enter__(self) -> "NodeLinkConverter":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


# --------- module-level helpers ---------
_converter_lock = threading.Lock()
_converter: Optional[NodeLinkConverter] = None


def _get_converter() -> NodeLinkConverter:
    global _converter
    with _converter_lock:
        if _converter is None:
            _converter = NodeLinkConverter()
        return _converter


def close_global_converter() -> None:
    global _converter
    with _converter_lock:
        if _converter is not None:
            try:
                _converter.close()
            finally:
                _converter = None


def convert_to_outbound_text(url: str, timeout: Optional[float] = None) -> Optional[str]:
    """
    دقیقاً همان JSON خطیِ خروجی JS را برمی‌گرداند (بدون parse/re-dump).
    """
    return _get_converter().convert(url, timeout=timeout)


def convert_to_outbound(url: str, timeout: Optional[float] = None) -> Union[Dict[str, Any], Any, str]:
    """
    سازگاری با کدهای قبلی:
      - اگر خروجی JSON معتبر باشد => dict/list
      - اگر parse نشد => همان string
      - اگر null/ERR/timeout => Exception
    """
    text = convert_to_outbound_text(url, timeout=timeout)
    if text is None:
        raise NodeBridgeError("Node bridge returned null/err or timed out.")
    try:
        return json.loads(text)
    except Exception:
        return text
