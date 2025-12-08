# app/xraymgr/jsbridge.py
import os
import subprocess
import threading
import tempfile
import select
from typing import Optional

from .settings import BASE_DIR


DEFAULT_NODE_TIMEOUT = 15.0


class NodeBridgeError(Exception):
    pass


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

function runFile(p) { const code = fs.readFileSync(p, 'utf8'); vm.runInThisContext(code, { filename: p }); }
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


class NodeLinkConverter:
    """
    بریج ساده به Node:
      - یک پردازه دائمی node با BRIDGE_JS بالا می‌آورد
      - WEB_BUNDLE_DIR را روی webbundle تنظیم می‌کند
      - برای هر لینک، یک خط JSON (یا 'null' / 'ERR:...') می‌گیرد
    """

    def __init__(
        self,
        scripts_dir: Optional[str] = None,
        node_path: Optional[str] = None,
        timeout: float = DEFAULT_NODE_TIMEOUT,
    ) -> None:
        # اول از env بخوانیم، اگر کاربر خواست override کند
        if scripts_dir is None:
            env_dir = os.environ.get("XRAYMGR_WEBBUNDLE_DIR")
            if env_dir:
                scripts_dir = env_dir
            else:
                # پیش‌فرض: ریشه‌ی پروژه = BASE_DIR.parent
                project_root = BASE_DIR.parent
                scripts_dir = os.path.join(str(project_root), "webbundle")

        self.scripts_dir = os.path.abspath(scripts_dir)
        if not os.path.isdir(self.scripts_dir):
            raise NodeBridgeError(f"WEB_BUNDLE_DIR not found: {self.scripts_dir}")

        # مسیر node: یا env خاص، یا آرگومان، یا فقط 'node'
        self.node_path = node_path or os.environ.get("XRAYMGR_NODE_PATH", "node")
        self.timeout = timeout

        self._proc: Optional[subprocess.Popen] = None
        self._bridge_path: Optional[str] = None
        self._lock = threading.Lock()
        self._stderr_thread: Optional[threading.Thread] = None

        self._start_process()

    # ---------- process management ----------

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
            raise NodeBridgeError(f"Failed to start node process: {e}") from e

        self._proc = proc

        self._stderr_thread = threading.Thread(target=self._drain_stderr, daemon=True)
        self._stderr_thread.start()

        ready_line = proc.stdout.readline()
        if not ready_line:
            self._kill_process_silently()
            raise NodeBridgeError("Node bridge exited before READY.")
        if ready_line.strip() != "READY":
            self._kill_process_silently()
            raise NodeBridgeError(f"Unexpected READY line from node: {ready_line!r}")

    def _drain_stderr(self) -> None:
        proc = self._proc
        if not proc or not proc.stderr:
            return
        try:
            for _ in proc.stderr:
                pass
        except Exception:
            pass

    def _kill_process_silently(self) -> None:
        proc = self._proc
        self._proc = None
        if proc is not None:
            try:
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

    # ---------- public API ----------

    def convert(self, link: str, timeout: Optional[float] = None) -> Optional[str]:
        if not link or not link.strip():
            return None

        to = timeout if timeout is not None else self.timeout

        with self._lock:
            proc = self._proc
            if proc is None or proc.poll() is not None:
                self._start_process()
                proc = self._proc
            if proc is None or proc.stdin is None or proc.stdout is None:
                raise NodeBridgeError("Node process is not available.")

            normalized = link.replace("\r", " ").replace("\n", " ")
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
                self._kill_process_silently()
                return None

            line = proc.stdout.readline()
            if not line:
                self._kill_process_silently()
                return None

            line = line.strip()
            if not line:
                return None
            if line.startswith("ERR:"):
                return None
            if line.lower() == "null":
                return None

            return line

    def close(self) -> None:
        self._kill_process_silently()

    def __enter__(self) -> "NodeLinkConverter":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
