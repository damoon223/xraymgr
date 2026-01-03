from __future__ import annotations

import contextlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import Body, FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
from starlette.staticfiles import StaticFiles

from .collector import SubscriptionCollector
from .compress_db import compress_db as compress_db_func
from .group_updater import ConfigGroupUpdater
from .hash_updater import ConfigHashUpdater
from .importer import RawConfigImporter
from .json_updater import JsonConfigUpdater
from .settings import get_db_path

try:
    from .json_repair_updater import JsonRepairUpdater  # type: ignore
except Exception:  # pragma: no cover
    JsonRepairUpdater = None  # type: ignore


app = FastAPI(title="XrayMgr Web Dashboard")

BASE_DIR = Path(__file__).resolve().parent
INDEX_HTML_PATH = BASE_DIR / "web_static" / "index.html"
STATIC_DIR = BASE_DIR / "web_static"

MAX_LOG_LINES = 5000
MAX_TEST_LOG_LINES = 20000

SETTINGS_ENV_VAR = "XRAYMGR_PANEL_SETTINGS_PATH"
SETTINGS_FILE_NAME = "panel_settings.json"

# مسیر واقعی روی سرور شما (x-ui)
XRAY_XUI_DEFAULT = "/usr/local/x-ui/bin/xray-linux-amd64"

DEFAULT_SETTINGS: Dict[str, Any] = {
    "version": 1,
    "sql_presets": [
        {"label": "۱۰ سطر اول links", "query": "select * from links limit 10"},
        {"label": "select count(*) from links", "query": "select count(*) from links"},
        {
            "label": "invalid=1 و unsupported=0 (50)",
            "query": "select url,config_json,is_invalid,is_protocol_unsupported from links where is_invalid=1 and is_protocol_unsupported=0 limit 50",
        },
        {"label": "select * from links where url='%%'", "query": "select * from links where url='%%'"},
        {"label": "delete from links", "query": "delete from links"},
    ],
    "test": {
        "count": 100,
        "parallel": 10,
        "port_start": 9000,
        "port_count": 100,
        "inbound_tag_prefix": "in_test_",
        "check_timeout_sec": 60,
        "socks_user": "me",
        "socks_pass": "1",
        # پیش‌فرض را مسیر واقعی xray شما می‌گذاریم تا داخل پنل هم مثل اجرای دستی کار کند
        "xray_bin": XRAY_XUI_DEFAULT,
        "api_server": "127.0.0.1:10085",
        "socks_listen": "127.0.0.1",
    },
    "base_routing": {},
}


class NoCacheStaticFiles(StaticFiles):
    async def get_response(self, path: str, scope):
        resp = await super().get_response(path, scope)
        if resp.status_code == 200:
            resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
            resp.headers["Pragma"] = "no-cache"
            resp.headers["Expires"] = "0"
        return resp


app.mount("/static", NoCacheStaticFiles(directory=STATIC_DIR), name="static")


# ----------------- DB helpers -----------------


def get_connection() -> sqlite3.Connection:
    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def run_query(query: str, params: Optional[List[Any]] = None) -> Dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        if params:
            cur.execute(query, params)
        else:
            cur.execute(query)

        if cur.description:
            columns = [d[0] for d in cur.description]
            rows = cur.fetchall()
            data_rows = [dict(zip(columns, row)) for row in rows]
        else:
            columns = []
            data_rows = []

        conn.commit()
        return {"columns": columns, "rows": data_rows, "rowcount": cur.rowcount}
    finally:
        conn.close()


# ----------------- Settings helpers -----------------


_settings_lock = threading.Lock()
_settings_cache: Optional[Dict[str, Any]] = None
_settings_cache_mtime: Optional[float] = None


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = dict(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)  # type: ignore[arg-type]
        else:
            out[k] = v
    return out


def _settings_path() -> Path:
    env = os.getenv(SETTINGS_ENV_VAR, "").strip()
    if env:
        return Path(env).expanduser().resolve()

    dbp = Path(get_db_path()).expanduser()
    if not dbp.is_absolute():
        dbp = (Path.cwd() / dbp).resolve()
    return dbp.parent / SETTINGS_FILE_NAME


def _atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8", dir=str(path.parent)) as tf:
        tf.write(text)
        tmp = Path(tf.name)
    tmp.replace(path)


def _ensure_settings_file_exists() -> None:
    path = _settings_path()
    if path.exists():
        return
    merged = dict(DEFAULT_SETTINGS)
    _atomic_write_text(path, json.dumps(merged, ensure_ascii=False, indent=2) + "\n")


def load_settings() -> Dict[str, Any]:
    _ensure_settings_file_exists()
    path = _settings_path()

    st = path.stat()
    mtime = st.st_mtime

    with _settings_lock:
        global _settings_cache, _settings_cache_mtime
        if _settings_cache is not None and _settings_cache_mtime == mtime:
            return dict(_settings_cache)

        raw = path.read_text(encoding="utf-8")
        try:
            parsed = json.loads(raw) if raw.strip() else {}
        except Exception:
            parsed = {}

        if not isinstance(parsed, dict):
            parsed = {}

        merged = _deep_merge(DEFAULT_SETTINGS, parsed)
        _settings_cache = merged
        _settings_cache_mtime = mtime
        return dict(merged)


def save_settings(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("settings must be a JSON object")

    merged = _deep_merge(DEFAULT_SETTINGS, payload)

    path = _settings_path()
    _atomic_write_text(path, json.dumps(merged, ensure_ascii=False, indent=2) + "\n")

    with _settings_lock:
        global _settings_cache, _settings_cache_mtime
        _settings_cache = None
        _settings_cache_mtime = None

    return merged


# ----------------- executable / env helpers -----------------


_STD_PATH_PARTS = [
    "/usr/local/sbin",
    "/usr/local/bin",
    "/usr/sbin",
    "/usr/bin",
    "/sbin",
    "/bin",
]


def _merged_std_path(base_path: Optional[str]) -> str:
    parts: List[str] = []
    if base_path:
        for p in str(base_path).split(":"):
            p = p.strip()
            if p and p not in parts:
                parts.append(p)
    for p in _STD_PATH_PARTS:
        if p not in parts:
            parts.append(p)
    return ":".join(parts)


def _subprocess_env_with_std_path() -> Dict[str, str]:
    env = dict(os.environ)
    env["PATH"] = _merged_std_path(env.get("PATH", ""))
    return env


def _subprocess_env_for_test() -> Dict[str, str]:
    """
    برای جلوگیری از side-effectهای xray که ممکن است از env مسیر config را بگیرد
    (مثل XRAY_LOCATION_CONFIG/ASSET/CONFDIR)، این‌ها را از محیط تست حذف می‌کنیم.
    """
    env = _subprocess_env_with_std_path()
    for k in (
        "XRAY_LOCATION_ASSET",
        "XRAY_LOCATION_CONFIG",
        "XRAY_LOCATION_CONFDIR",
        "xray.location.asset",
        "xray.location.config",
        "xray.location.confdir",
        "V2RAY_LOCATION_ASSET",
        "V2RAY_LOCATION_CONFIG",
    ):
        env.pop(k, None)
    return env


def _is_exec_file(p: str) -> bool:
    try:
        return bool(p) and os.path.isfile(p) and os.access(p, os.X_OK)
    except Exception:
        return False


def _resolve_xray_bin_for_panel(xray_bin_cfg: str) -> Optional[str]:
    """
    منطق قطعی (بدون auto-discovery چندمسیره):
    - اگر مسیر مطلق/دارای '/' باشد: فقط همان را validate می‌کنیم.
    - اگر مقدار 'xray' باشد:
        - اول اگر در PATH همین پروسه resolve شد استفاده می‌کنیم
        - اگر نشد، فقط fallback ثابتِ x-ui را (XRAY_XUI_DEFAULT) چک می‌کنیم
    """
    xray_bin_cfg = (xray_bin_cfg or "").strip()
    if not xray_bin_cfg:
        xray_bin_cfg = "xray"

    if os.path.isabs(xray_bin_cfg) or "/" in xray_bin_cfg:
        return xray_bin_cfg if _is_exec_file(xray_bin_cfg) else None

    # اگر کاربر فقط نام داده باشد، فقط PATH همین پروسه را چک می‌کنیم
    merged_path = _merged_std_path(os.environ.get("PATH", ""))
    found = shutil.which(xray_bin_cfg, path=merged_path)
    if found and _is_exec_file(found):
        return found

    # fallback ثابت (همان مسیری که شما پیدا کردید)
    if _is_exec_file(XRAY_XUI_DEFAULT):
        return XRAY_XUI_DEFAULT

    return None


# ----------------- Background jobs state -----------------


jobs_lock = threading.Lock()

collector_state: Dict[str, Any] = {"name": "collector", "running": False, "last_started_at": None, "last_finished_at": None, "stats": None, "instance": None}
importer_state: Dict[str, Any] = {"name": "importer", "running": False, "last_started_at": None, "last_finished_at": None, "stats": None, "instance": None}
json_state: Dict[str, Any] = {"name": "json", "running": False, "last_started_at": None, "last_finished_at": None, "stats": None, "instance": None}
hash_state: Dict[str, Any] = {"name": "hash", "running": False, "last_started_at": None, "last_finished_at": None, "stats": None, "instance": None}
group_state: Dict[str, Any] = {"name": "group", "running": False, "last_started_at": None, "last_finished_at": None, "stats": None, "instance": None}
json_repair_state: Dict[str, Any] = {"name": "json_repair", "running": False, "last_started_at": None, "last_finished_at": None, "stats": None, "instance": None}
compress_state: Dict[str, Any] = {"name": "compress", "running": False, "last_started_at": None, "last_finished_at": None, "stats": None, "instance": None}
test_state: Dict[str, Any] = {"name": "test", "running": False, "last_started_at": None, "last_finished_at": None, "stats": None, "instance": None}

shared_log: List[str] = []
test_log: List[str] = []


class SharedLogStream:
    def __init__(self) -> None:
        self._buf = ""

    def write(self, s: str) -> int:
        if s is None:
            return 0
        text = str(s)
        try:
            sys.__stdout__.write(text)
        except Exception:
            pass

        self._buf += text
        lines = self._buf.split("\n")
        self._buf = lines[-1]

        to_add = [ln.rstrip("\r") for ln in lines[:-1]]
        if to_add:
            with jobs_lock:
                for ln in to_add:
                    if ln.strip():
                        shared_log.append(ln)
                if len(shared_log) > MAX_LOG_LINES:
                    del shared_log[: len(shared_log) - MAX_LOG_LINES]
        return len(text)

    def flush(self) -> None:
        try:
            sys.__stdout__.flush()
        except Exception:
            pass


def _reset_log() -> None:
    with jobs_lock:
        shared_log.clear()


def _reset_test_log() -> None:
    with jobs_lock:
        test_log.clear()


def _append_test_log_line(line: str) -> None:
    ln = str(line).rstrip("\r\n")
    if not ln.strip():
        return
    with jobs_lock:
        test_log.append(ln)
        if len(test_log) > MAX_TEST_LOG_LINES:
            del test_log[: len(test_log) - MAX_TEST_LOG_LINES]


def _mark_job_start(state: Dict[str, Any], instance: Any) -> None:
    with jobs_lock:
        state["running"] = True
        state["last_started_at"] = time.time()
        state["last_finished_at"] = None
        state["stats"] = None
        state["instance"] = instance


def _mark_job_finish(state: Dict[str, Any], stats: Optional[Dict[str, Any]] = None) -> None:
    with jobs_lock:
        state["running"] = False
        state["last_finished_at"] = time.time()
        state["stats"] = stats
        state["instance"] = None


def _job_status_payload(state: Dict[str, Any]) -> Dict[str, Any]:
    with jobs_lock:
        return {
            "running": bool(state["running"]),
            "last_started_at": state["last_started_at"],
            "last_finished_at": state["last_finished_at"],
            "stats": state["stats"],
        }


# ----------------- Job threads -----------------


def _run_collector_thread() -> None:
    log_stream = SharedLogStream()
    _reset_log()

    collector = SubscriptionCollector()
    _mark_job_start(collector_state, collector)

    try:
        with contextlib.redirect_stdout(log_stream):
            collector.collect_from_sources_file()
        _mark_job_finish(collector_state, {"collector_stats": getattr(collector, "stats", None)})
    except Exception as e:
        log_stream.write(f"[collector] FATAL ERROR: {e}\n")
        _mark_job_finish(collector_state, {"error": str(e)})


def _run_importer_thread() -> None:
    log_stream = SharedLogStream()
    _reset_log()

    importer = RawConfigImporter(batch_size=1000)
    _mark_job_start(importer_state, importer)

    try:
        with contextlib.redirect_stdout(log_stream):
            importer.import_configs()
        _mark_job_finish(importer_state, {"importer_stats": getattr(importer, "stats", None)})
    except Exception as e:
        log_stream.write(f"[importer] FATAL ERROR: {e}\n")
        _mark_job_finish(importer_state, {"error": str(e)})


def _run_json_thread() -> None:
    log_stream = SharedLogStream()
    _reset_log()

    updater = JsonConfigUpdater(batch_size=1000)
    _mark_job_start(json_state, updater)

    try:
        with contextlib.redirect_stdout(log_stream):
            updater.update_missing_json()
        _mark_job_finish(json_state, {"json_stats": getattr(updater, "stats", None)})
    except Exception as e:
        log_stream.write(f"[json_updater] FATAL ERROR: {e}\n")
        _mark_job_finish(json_state, {"error": str(e)})


def _run_hash_thread() -> None:
    log_stream = SharedLogStream()
    _reset_log()

    updater = ConfigHashUpdater(batch_size=1000)
    _mark_job_start(hash_state, updater)

    try:
        with contextlib.redirect_stdout(log_stream):
            updater.update_hashes()
        _mark_job_finish(hash_state, {"hash_stats": getattr(updater, "stats", None)})
    except Exception as e:
        log_stream.write(f"[hash_updater] FATAL ERROR: {e}\n")
        _mark_job_finish(hash_state, {"error": str(e)})


def _run_group_thread() -> None:
    log_stream = SharedLogStream()
    _reset_log()

    updater = ConfigGroupUpdater(batch_size=500)
    _mark_job_start(group_state, updater)

    try:
        with contextlib.redirect_stdout(log_stream):
            updater.update_groups()
        _mark_job_finish(group_state, {"group_stats": getattr(updater, "stats", None)})
    except Exception as e:
        log_stream.write(f"[group_updater] FATAL ERROR: {e}\n")
        _mark_job_finish(group_state, {"error": str(e)})


def _run_json_repair_thread() -> None:
    log_stream = SharedLogStream()
    _reset_log()

    if JsonRepairUpdater is None:
        with contextlib.redirect_stdout(log_stream):
            print("[json_repair] JsonRepairUpdater module not available.")
        _mark_job_finish(json_repair_state, {"error": "JsonRepairUpdater not available"})
        return

    updater = JsonRepairUpdater(batch_size=1000)  # type: ignore
    _mark_job_start(json_repair_state, updater)

    try:
        with contextlib.redirect_stdout(log_stream):
            if hasattr(updater, "run"):
                updater.run()
            elif hasattr(updater, "repair"):
                updater.repair()
            elif hasattr(updater, "repair_invalid_then_convert"):
                updater.repair_invalid_then_convert()
            else:
                raise RuntimeError("JsonRepairUpdater has no runnable entry method")
        _mark_job_finish(json_repair_state, {"json_repair_stats": getattr(updater, "stats", None)})
    except Exception as e:
        log_stream.write(f"[json_repair] FATAL ERROR: {e}\n")
        _mark_job_finish(json_repair_state, {"error": str(e)})


def _run_compress_thread() -> None:
    log_stream = SharedLogStream()
    _reset_log()

    _mark_job_start(compress_state, instance=True)

    try:
        with contextlib.redirect_stdout(log_stream):
            db_path = os.path.abspath(get_db_path())
            db_dir = os.path.dirname(db_path)
            base = os.path.basename(db_path)
            ts = time.strftime("%Y%m%d-%H%M%S")
            out_path = os.path.join(db_dir, f"{base}.{ts}.xz")
            compress_db_func(db_path, out_path)
        _mark_job_finish(compress_state, {"output": out_path})
    except Exception as e:
        log_stream.write(f"[compress] FATAL ERROR: {e}\n")
        _mark_job_finish(compress_state, {"error": str(e)})


# ----------------- Test job -----------------


class TestProcess:
    def __init__(self, proc: subprocess.Popen) -> None:
        self.proc = proc

    def request_stop(self) -> None:
        try:
            if self.proc.poll() is None:
                self.proc.terminate()
        except Exception:
            pass


_test_arg_cache_lock = threading.Lock()
_test_arg_cache: Dict[str, bool] = {}


def _script_supports_arg(script_path: Path, arg: str) -> bool:
    key = f"{script_path}:{arg}"
    with _test_arg_cache_lock:
        if key in _test_arg_cache:
            return _test_arg_cache[key]

    try:
        r = subprocess.run([sys.executable or "python3", str(script_path), "-h"], capture_output=True, text=True, timeout=3)
        out = (r.stdout or "") + (r.stderr or "")
        ok = arg in out
    except Exception:
        ok = False

    with _test_arg_cache_lock:
        _test_arg_cache[key] = ok
    return ok


class TestRunRequest(BaseModel):
    count: Optional[int] = None
    parallel: Optional[int] = None


def _run_test_thread(req: TestRunRequest) -> None:
    _reset_test_log()

    settings = load_settings()
    tcfg = settings.get("test") if isinstance(settings, dict) else {}
    if not isinstance(tcfg, dict):
        tcfg = {}

    count = int(req.count) if req.count else int(tcfg.get("count", 100))
    if count <= 0:
        count = 1

    parallel = int(req.parallel) if req.parallel else int(tcfg.get("parallel", 10))
    if parallel <= 0:
        parallel = 1

    script_path = BASE_DIR / "test_batch_10.py"
    if not script_path.exists():
        _append_test_log_line("[test] ERROR: test_batch_10.py not found in app/xraymgr/")
        _mark_job_finish(test_state, {"error": "test_batch_10.py not found"})
        return

    xray_bin_cfg = str(tcfg.get("xray_bin", XRAY_XUI_DEFAULT) or "").strip()
    resolved_xray = _resolve_xray_bin_for_panel(xray_bin_cfg)

    if not resolved_xray:
        merged_path = _merged_std_path(os.environ.get("PATH", ""))
        _append_test_log_line(f"[test] FAIL(preflight) xray_bin_not_resolvable xray_bin={xray_bin_cfg!r}")
        _append_test_log_line(f"[test] PATH_used={merged_path}")
        _append_test_log_line(f"[test] expected_exec={XRAY_XUI_DEFAULT!r} exists_exec={_is_exec_file(XRAY_XUI_DEFAULT)}")
        _mark_job_finish(test_state, {"error": "xray_bin_not_resolvable", "xray_bin": xray_bin_cfg})
        return

    py = sys.executable or "python3"
    db_path = os.path.abspath(get_db_path())

    cmd: List[str] = [py, "-u", str(script_path), "--db", db_path, "--count", str(count)]

    if _script_supports_arg(script_path, "--parallel"):
        cmd += ["--parallel", str(parallel)]

    if _script_supports_arg(script_path, "--xray-bin"):
        cmd += ["--xray-bin", str(resolved_xray)]

    api_server = tcfg.get("api_server")
    if api_server and _script_supports_arg(script_path, "--api-server"):
        cmd += ["--api-server", str(api_server)]

    socks_listen = tcfg.get("socks_listen")
    if socks_listen and _script_supports_arg(script_path, "--socks-listen"):
        cmd += ["--socks-listen", str(socks_listen)]

    check_timeout = tcfg.get("check_timeout_sec")
    if check_timeout and _script_supports_arg(script_path, "--check-timeout"):
        cmd += ["--check-timeout", str(check_timeout)]

    _append_test_log_line(f"[test] start count={count} parallel={parallel} db={db_path}")
    _append_test_log_line(f"[test] resolved_xray={resolved_xray}")
    _append_test_log_line(f"[test] cmd={' '.join(cmd)}")

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True,
            env=_subprocess_env_for_test(),
        )

        with jobs_lock:
            test_state["instance"] = TestProcess(proc)

        assert proc.stdout is not None
        for line in proc.stdout:
            _append_test_log_line(line)

        rc = proc.wait()
        _append_test_log_line(f"[test] finished rc={rc}")
        _mark_job_finish(test_state, {"rc": rc, "count": count, "parallel": parallel})
    except Exception as e:
        _append_test_log_line(f"[test] FATAL ERROR: {e}")
        _mark_job_finish(test_state, {"error": str(e)})


# ----------------- Routes -----------------


class SQLQuery(BaseModel):
    query: str
    params: Optional[List[Any]] = None


class SettingsPayload(BaseModel):
    settings: Dict[str, Any]


@app.get("/", response_class=HTMLResponse)
async def index():
    if not INDEX_HTML_PATH.exists():
        raise HTTPException(status_code=500, detail="index.html not found")
    html = INDEX_HTML_PATH.read_text(encoding="utf-8")
    return HTMLResponse(
        content=html,
        headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0", "Pragma": "no-cache", "Expires": "0"},
    )


@app.get("/health")
async def health():
    db_path = get_db_path()
    ok = os.path.exists(db_path)
    return {"status": "ok" if ok else "missing", "db_path": db_path}


@app.post("/db/query")
async def post_query(payload: SQLQuery):
    q = payload.query or ""
    if not q.strip():
        raise HTTPException(status_code=400, detail="Empty query.")
    try:
        data = run_query(q, payload.params)
        return JSONResponse(data)
    except sqlite3.Error as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/settings")
async def get_settings():
    st = load_settings()
    return {"path": str(_settings_path()), "settings": st}


@app.put("/settings")
async def put_settings(payload: SettingsPayload = Body(...)):
    try:
        merged = save_settings(payload.settings)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"status": "saved", "path": str(_settings_path()), "settings": merged}


@app.get("/collector/log")
async def collector_log(offset: int = Query(0, ge=0)):
    with jobs_lock:
        total = len(shared_log)
        if offset < 0 or offset > total:
            offset = 0
        lines = shared_log[offset:]
    return {"offset": offset, "total": total, "lines": lines}


@app.get("/jobs/summary")
async def jobs_summary():
    with jobs_lock:
        any_running = any(
            s["running"]
            for s in (
                collector_state,
                importer_state,
                json_state,
                hash_state,
                group_state,
                json_repair_state,
                compress_state,
                test_state,
            )
        )
    return {
        "db_path": get_db_path(),
        "any_running": any_running,
        "jobs": {
            "collector": _job_status_payload(collector_state),
            "importer": _job_status_payload(importer_state),
            "json": _job_status_payload(json_state),
            "hash": _job_status_payload(hash_state),
            "group": _job_status_payload(group_state),
            "json_repair": _job_status_payload(json_repair_state),
            "compress": _job_status_payload(compress_state),
            "test": _job_status_payload(test_state),
        },
    }


def _start_thread_if_idle(state: Dict[str, Any], target) -> None:
    with jobs_lock:
        if state["running"]:
            raise HTTPException(status_code=409, detail=f"{state['name']} is already running")
        t = threading.Thread(target=target, daemon=True)
        t.start()


@app.post("/collector/run")
async def run_collector():
    _start_thread_if_idle(collector_state, _run_collector_thread)
    return {"status": "started"}


@app.post("/collector/stop")
async def stop_collector():
    with jobs_lock:
        if not collector_state["running"]:
            raise HTTPException(status_code=409, detail="Collector is not running")
        inst = collector_state.get("instance")
    if inst is None or not hasattr(inst, "request_stop"):
        raise HTTPException(status_code=500, detail="Collector instance stop not available")
    inst.request_stop()
    return {"status": "stopping"}


@app.get("/collector/status")
async def collector_status():
    return _job_status_payload(collector_state)


@app.post("/importer/run")
async def run_importer():
    _start_thread_if_idle(importer_state, _run_importer_thread)
    return {"status": "started"}


@app.post("/importer/stop")
async def stop_importer():
    with jobs_lock:
        if not importer_state["running"]:
            raise HTTPException(status_code=409, detail="Importer is not running")
        inst = importer_state.get("instance")
    if inst is None or not hasattr(inst, "request_stop"):
        raise HTTPException(status_code=500, detail="Importer instance stop not available")
    inst.request_stop()
    return {"status": "stopping"}


@app.get("/importer/status")
async def importer_status():
    return _job_status_payload(importer_state)


@app.post("/json/run")
async def run_json():
    _start_thread_if_idle(json_state, _run_json_thread)
    return {"status": "started"}


@app.post("/json/stop")
async def stop_json():
    with jobs_lock:
        if not json_state["running"]:
            raise HTTPException(status_code=409, detail="JSON updater is not running")
        inst = json_state.get("instance")
    if inst is None or not hasattr(inst, "request_stop"):
        raise HTTPException(status_code=500, detail="JSON updater instance stop not available")
    inst.request_stop()
    return {"status": "stopping"}


@app.get("/json/status")
async def json_status():
    return _job_status_payload(json_state)


@app.post("/hash/run")
async def run_hash():
    _start_thread_if_idle(hash_state, _run_hash_thread)
    return {"status": "started"}


@app.post("/hash/stop")
async def stop_hash():
    with jobs_lock:
        if not hash_state["running"]:
            raise HTTPException(status_code=409, detail="Hash updater is not running")
        inst = hash_state.get("instance")
    if inst is None or not hasattr(inst, "request_stop"):
        raise HTTPException(status_code=500, detail="Hash updater instance stop not available")
    inst.request_stop()
    return {"status": "stopping"}


@app.get("/hash/status")
async def hash_status():
    return _job_status_payload(hash_state)


@app.post("/group/run")
async def run_group():
    _start_thread_if_idle(group_state, _run_group_thread)
    return {"status": "started"}


@app.post("/group/stop")
async def stop_group():
    with jobs_lock:
        if not group_state["running"]:
            raise HTTPException(status_code=409, detail="Group updater is not running")
        inst = group_state.get("instance")
    if inst is None or not hasattr(inst, "request_stop"):
        raise HTTPException(status_code=500, detail="Group updater instance stop not available")
    inst.request_stop()
    return {"status": "stopping"}


@app.get("/group/status")
async def group_status():
    return _job_status_payload(group_state)


@app.post("/json_repair/run")
async def run_json_repair():
    _start_thread_if_idle(json_repair_state, _run_json_repair_thread)
    return {"status": "started"}


@app.post("/json_repair/stop")
async def stop_json_repair():
    with jobs_lock:
        if not json_repair_state["running"]:
            raise HTTPException(status_code=409, detail="JSON repair is not running")
        inst = json_repair_state.get("instance")
    if inst is None or not hasattr(inst, "request_stop"):
        return {"status": "no_stop_supported"}
    inst.request_stop()
    return {"status": "stopping"}


@app.get("/json_repair/status")
async def json_repair_status():
    return _job_status_payload(json_repair_state)


@app.post("/compress/run")
async def run_compress():
    _start_thread_if_idle(compress_state, _run_compress_thread)
    return {"status": "started"}


@app.get("/compress/status")
async def compress_status():
    return _job_status_payload(compress_state)


@app.get("/test/status")
async def test_status():
    return _job_status_payload(test_state)


@app.get("/test/log")
async def test_log_endpoint(offset: int = Query(0, ge=0)):
    with jobs_lock:
        total = len(test_log)
        if offset < 0 or offset > total:
            offset = 0
        lines = test_log[offset:]
    return {"offset": offset, "total": total, "lines": lines}


@app.post("/test/run")
async def test_run(req: TestRunRequest = Body(default_factory=TestRunRequest)):
    with jobs_lock:
        if test_state["running"]:
            raise HTTPException(status_code=409, detail="Test is already running")
        test_state["running"] = True
        test_state["last_started_at"] = time.time()
        test_state["last_finished_at"] = None
        test_state["stats"] = None
        test_state["instance"] = None
        test_log.clear()

        t = threading.Thread(target=_run_test_thread, args=(req,), daemon=True)
        t.start()

    return {"status": "started"}


@app.post("/test/stop")
async def test_stop():
    with jobs_lock:
        if not test_state["running"]:
            raise HTTPException(status_code=409, detail="Test is not running")
        inst = test_state.get("instance")

    if inst is not None and hasattr(inst, "request_stop"):
        inst.request_stop()
        return {"status": "stopping"}

    return {"status": "stopping"}


def get_app():
    return app
