from __future__ import annotations

import contextlib
import os
import sqlite3
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
from starlette.staticfiles import StaticFiles

from .settings import get_db_path
from .collector import SubscriptionCollector
from .importer import RawConfigImporter
from .json_updater import JsonConfigUpdater
from .hash_updater import ConfigHashUpdater
from .group_updater import ConfigGroupUpdater

# json_repair ممکن است در بعضی نسخه‌ها وجود داشته باشد
try:
    from .json_repair_updater import JsonRepairUpdater  # type: ignore
except Exception:  # pragma: no cover
    JsonRepairUpdater = None  # type: ignore

# compress ممکن است به شکل ماژول/اسکریپت وجود داشته باشد
try:
    from .compress_db import compress_db as compress_db_func  # type: ignore
except Exception:  # pragma: no cover
    compress_db_func = None  # type: ignore


app = FastAPI(title="XrayMgr Web Dashboard")

BASE_DIR = Path(__file__).resolve().parent
INDEX_HTML_PATH = BASE_DIR / "web_static" / "index.html"
STATIC_DIR = BASE_DIR / "web_static"

MAX_LOG_LINES = 5000


class NoCacheStaticFiles(StaticFiles):
    async def get_response(self, path: str, scope):
        resp = await super().get_response(path, scope)
        # جلوگیری از کش شدن JS/CSS تا مجبور به Ctrl+F5 نباشید
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

        return {
            "columns": columns,
            "rows": data_rows,
            "rowcount": cur.rowcount,
        }
    finally:
        conn.close()


# ----------------- Background jobs state -----------------


jobs_lock = threading.Lock()

collector_state: Dict[str, Any] = {
    "name": "collector",
    "running": False,
    "last_started_at": None,
    "last_finished_at": None,
    "stats": None,
    "instance": None,
}

importer_state: Dict[str, Any] = {
    "name": "importer",
    "running": False,
    "last_started_at": None,
    "last_finished_at": None,
    "stats": None,
    "instance": None,
}

json_state: Dict[str, Any] = {
    "name": "json",
    "running": False,
    "last_started_at": None,
    "last_finished_at": None,
    "stats": None,
    "instance": None,
}

hash_state: Dict[str, Any] = {
    "name": "hash",
    "running": False,
    "last_started_at": None,
    "last_finished_at": None,
    "stats": None,
    "instance": None,
}

group_state: Dict[str, Any] = {
    "name": "group",
    "running": False,
    "last_started_at": None,
    "last_finished_at": None,
    "stats": None,
    "instance": None,
}

json_repair_state: Dict[str, Any] = {
    "name": "json_repair",
    "running": False,
    "last_started_at": None,
    "last_finished_at": None,
    "stats": None,
    "instance": None,
}

compress_state: Dict[str, Any] = {
    "name": "compress",
    "running": False,
    "last_started_at": None,
    "last_finished_at": None,
    "stats": None,
    "instance": None,
}

# یک لاگ مشترک برای همه jobها (پنل فقط از یک endpoint لاگ می‌گیرد)
shared_log: List[str] = []


class SharedLogStream:
    """
    stream برای جمع‌کردن خروجی print به شکل خط‌به‌خط.
    مشکل پشت‌سرهم شدن لاگ‌ها معمولاً از این است که write() تکه‌تکه صدا می‌خورد.
    این کلاس بافر می‌کند تا فقط خط کامل را append کند.
    """

    def __init__(self) -> None:
        self._buf = ""

    def write(self, s: str) -> int:
        if s is None:
            return 0
        text = str(s)
        # روی کنسول واقعی هم چاپ کن
        try:
            sys.__stdout__.write(text)
        except Exception:
            pass

        self._buf += text
        lines = self._buf.split("\n")
        self._buf = lines[-1]  # تکهٔ ناقص آخر

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
            # اسم متد را تلاش می‌کنیم با چند حالت رایج
            if hasattr(updater, "run"):
                updater.run()
            elif hasattr(updater, "repair"):
                updater.repair()
            elif hasattr(updater, "repair_invalid_then_convert"):
                updater.repair_invalid_then_convert()
            else:
                raise RuntimeError("JsonRepairUpdater has no runnable entry method (run/repair/repair_invalid_then_convert)")
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

            if compress_db_func is None:
                raise RuntimeError("compress_db() not available")
            compress_db_func(db_path, out_path)  # type: ignore

        _mark_job_finish(compress_state, {"output": out_path})
    except Exception as e:
        log_stream.write(f"[compress] FATAL ERROR: {e}\n")
        _mark_job_finish(compress_state, {"error": str(e)})


# ----------------- Pydantic models -----------------


class SQLQuery(BaseModel):
    query: str
    params: Optional[List[Any]] = None


# ----------------- Routes: UI -----------------


@app.get("/", response_class=HTMLResponse)
async def index():
    try:
        html = INDEX_HTML_PATH.read_text(encoding="utf-8")
    except FileNotFoundError:
        raise HTTPException(status_code=500, detail="index.html not found")
    return HTMLResponse(
        content=html,
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


# ----------------- Routes: API -----------------


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


# ---- Shared log endpoint (پنل فقط همین را می‌خواند) ----


@app.get("/collector/log")
async def collector_log(offset: int = Query(0, ge=0)):
    with jobs_lock:
        total = len(shared_log)
        if offset < 0 or offset > total:
            offset = 0
        lines = shared_log[offset:]
    return {"offset": offset, "total": total, "lines": lines}


# ---- Unified summary endpoint (برای کاهش تعداد requestها) ----


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
        },
    }


# ---- Collector endpoints ----


@app.post("/collector/run")
async def run_collector():
    with jobs_lock:
        if collector_state["running"]:
            raise HTTPException(status_code=409, detail="Collector is already running")
        t = threading.Thread(target=_run_collector_thread, daemon=True)
        t.start()
    return {"status": "started"}


@app.post("/collector/stop")
async def stop_collector():
    with jobs_lock:
        if not collector_state["running"]:
            raise HTTPException(status_code=409, detail="Collector is not running")
        inst = collector_state.get("instance")
    if inst is None:
        raise HTTPException(status_code=500, detail="Collector instance not available")
    inst.request_stop()
    return {"status": "stopping"}


@app.get("/collector/status")
async def collector_status():
    return _job_status_payload(collector_state)


# ---- Importer endpoints ----


@app.post("/importer/run")
async def run_importer():
    with jobs_lock:
        if importer_state["running"]:
            raise HTTPException(status_code=409, detail="Importer is already running")
        t = threading.Thread(target=_run_importer_thread, daemon=True)
        t.start()
    return {"status": "started"}


@app.post("/importer/stop")
async def stop_importer():
    with jobs_lock:
        if not importer_state["running"]:
            raise HTTPException(status_code=409, detail="Importer is not running")
        inst = importer_state.get("instance")
    if inst is None:
        raise HTTPException(status_code=500, detail="Importer instance not available")
    inst.request_stop()
    return {"status": "stopping"}


@app.get("/importer/status")
async def importer_status():
    return _job_status_payload(importer_state)


# ---- JSON updater endpoints ----


@app.post("/json/run")
async def run_json():
    with jobs_lock:
        if json_state["running"]:
            raise HTTPException(status_code=409, detail="JSON updater is already running")
        t = threading.Thread(target=_run_json_thread, daemon=True)
        t.start()
    return {"status": "started"}


@app.post("/json/stop")
async def stop_json():
    with jobs_lock:
        if not json_state["running"]:
            raise HTTPException(status_code=409, detail="JSON updater is not running")
        inst = json_state.get("instance")
    if inst is None:
        raise HTTPException(status_code=500, detail="JSON updater instance not available")
    inst.request_stop()
    return {"status": "stopping"}


@app.get("/json/status")
async def json_status():
    return _job_status_payload(json_state)


# ---- JSON repair endpoints ----


@app.post("/json_repair/run")
async def run_json_repair():
    with jobs_lock:
        if json_repair_state["running"]:
            raise HTTPException(status_code=409, detail="JSON repair is already running")
        t = threading.Thread(target=_run_json_repair_thread, daemon=True)
        t.start()
    return {"status": "started"}


@app.post("/json_repair/stop")
async def stop_json_repair():
    with jobs_lock:
        if not json_repair_state["running"]:
            raise HTTPException(status_code=409, detail="JSON repair is not running")
        inst = json_repair_state.get("instance")
    if inst is None:
        raise HTTPException(status_code=500, detail="JSON repair instance not available")
    # اگر کلاس شما request_stop دارد:
    if hasattr(inst, "request_stop"):
        inst.request_stop()
        return {"status": "stopping"}
    return {"status": "no_stop_supported"}


@app.get("/json_repair/status")
async def json_repair_status():
    return _job_status_payload(json_repair_state)


# ---- Hash updater endpoints ----


@app.post("/hash/run")
async def run_hash():
    with jobs_lock:
        if hash_state["running"]:
            raise HTTPException(status_code=409, detail="Hash updater is already running")
        t = threading.Thread(target=_run_hash_thread, daemon=True)
        t.start()
    return {"status": "started"}


@app.post("/hash/stop")
async def stop_hash():
    with jobs_lock:
        if not hash_state["running"]:
            raise HTTPException(status_code=409, detail="Hash updater is not running")
        inst = hash_state.get("instance")
    if inst is None:
        raise HTTPException(status_code=500, detail="Hash updater instance not available")
    inst.request_stop()
    return {"status": "stopping"}


@app.get("/hash/status")
async def hash_status():
    return _job_status_payload(hash_state)


# ---- Group updater endpoints ----


@app.post("/group/run")
async def run_group():
    with jobs_lock:
        if group_state["running"]:
            raise HTTPException(status_code=409, detail="Group updater is already running")
        t = threading.Thread(target=_run_group_thread, daemon=True)
        t.start()
    return {"status": "started"}


@app.post("/group/stop")
async def stop_group():
    with jobs_lock:
        if not group_state["running"]:
            raise HTTPException(status_code=409, detail="Group updater is not running")
        inst = group_state.get("instance")
    if inst is None:
        raise HTTPException(status_code=500, detail="Group updater instance not available")
    inst.request_stop()
    return {"status": "stopping"}


@app.get("/group/status")
async def group_status():
    return _job_status_payload(group_state)


# ---- Compress endpoints ----


@app.post("/compress/run")
async def run_compress():
    with jobs_lock:
        if compress_state["running"]:
            raise HTTPException(status_code=409, detail="Compress is already running")
        t = threading.Thread(target=_run_compress_thread, daemon=True)
        t.start()
    return {"status": "started"}


@app.get("/compress/status")
async def compress_status():
    return _job_status_payload(compress_state)


def get_app():
    return app
