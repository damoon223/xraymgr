from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import List, Optional, Dict, Any

import sqlite3
import os
import threading
import time
import sys
import contextlib
from pathlib import Path
from collections import deque

from .settings import get_db_path
from .collector import SubscriptionCollector
from .importer import RawConfigImporter
from .json_updater import JsonConfigUpdater
from .hash_updater import ConfigHashUpdater
from .compress_db import main as compress_db_main

app = FastAPI(title="XrayMgr Web Dashboard")

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "web_static"

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


# ----------------- DB helpers -----------------


def get_connection() -> sqlite3.Connection:
    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def list_tables() -> List[str]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name NOT LIKE 'sqlite_%' "
            "ORDER BY name"
        )
        rows = cur.fetchall()
        return [r["name"] for r in rows]
    finally:
        conn.close()


def fetch_table(name: str, limit: int = 100) -> Dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {name} LIMIT ?", (limit,))
        rows = cur.fetchall()
        columns = [d[0] for d in cur.description] if cur.description else []
        data = [dict(zip(columns, row)) for row in rows]
        return {"columns": columns, "rows": data}
    finally:
        conn.close()


def run_query(query: str, params: Optional[List[Any]] = None) -> Dict[str, Any]:
    """
    اجرای هر نوع کوئری SQLite:
      - برای SELECT / PRAGMA / ... داده و ستون‌ها را برمی‌گرداند.
      - برای UPDATE / DELETE / INSERT / DDL، فقط rowcount معنی‌دار است.
    """
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


collector_lock = threading.Lock()

# حداکثر تعداد خطوط لاگ که در حافظه نگه می‌داریم
MAX_LOG_LINES = 50
job_log_buffer: "deque[str]" = deque(maxlen=MAX_LOG_LINES)

collector_state = {
    "running": False,
    "last_started_at": None,
    "last_finished_at": None,
    "stats": None,      # dict یا None
    "log": job_log_buffer,  # اشاره به بافر حلقه‌ای
    "instance": None,   # SubscriptionCollector یا None
}

importer_state = {
    "running": False,
    "last_started_at": None,
    "last_finished_at": None,
    "stats": None,      # dict یا None
    "instance": None,   # RawConfigImporter یا None
}

json_state = {
    "running": False,
    "last_started_at": None,
    "last_finished_at": None,
    "stats": None,      # dict یا None
    "instance": None,   # JsonConfigUpdater یا None
}

hash_state = {
    "running": False,
    "last_started_at": None,
    "last_finished_at": None,
    "stats": None,      # dict یا None
    "instance": None,   # ConfigHashUpdater یا None
}

compress_state = {
    "running": False,
    "last_started_at": None,
    "last_finished_at": None,
    "stats": None,      # dict یا None
    "instance": None,   # برای سازگاری؛ استاپ نداریم فعلاً
}


class CollectorLogStream:
    """
    استریم ساده برای گرفتن خروجی printهای jobها (کالکتور/ایمپورتر/json_updater/hash_updater/compress):
      - هم‌زمان روی stdout واقعی می‌نویسد
      - علاوه بر آن، لاگ‌ها را در job_log_buffer (و collector_state["log"]) ذخیره می‌کند
    """

    def write(self, s: str) -> int:
        if not s:
            return 0
        sys.__stdout__.write(s)
        lines = s.splitlines()
        with collector_lock:
            for line in lines:
                if line.strip():
                    job_log_buffer.append(line)
        return len(s)

    def flush(self) -> None:
        sys.__stdout__.flush()


def _run_collector_thread():
    collector = SubscriptionCollector()

    log_stream = CollectorLogStream()
    with collector_lock:
        collector_state["running"] = True
        collector_state["last_started_at"] = time.time()
        collector_state["last_finished_at"] = None
        collector_state["stats"] = None
        collector_state["log"].clear()
        collector_state["instance"] = collector

    try:
        with contextlib.redirect_stdout(log_stream):
            collector.collect_from_sources_file()

        with collector_lock:
            collector_state["stats"] = {
                "collector_stats": collector.stats,
            }
    except Exception as e:
        log_stream.write(f"\n[collector] FATAL ERROR: {e}\n")
    finally:
        with collector_lock:
            collector_state["running"] = False
            collector_state["last_finished_at"] = time.time()
            collector_state["instance"] = None


def _run_importer_thread():
    importer = RawConfigImporter(batch_size=1000)

    log_stream = CollectorLogStream()
    with collector_lock:
        importer_state["running"] = True
        importer_state["last_started_at"] = time.time()
        importer_state["last_finished_at"] = None
        importer_state["stats"] = None
        importer_state["instance"] = importer
        collector_state["log"].clear()

    try:
        with contextlib.redirect_stdout(log_stream):
            importer.import_configs()

        with collector_lock:
            importer_state["stats"] = {
                "importer_stats": importer.stats,
            }
    except Exception as e:
        log_stream.write(f"\n[importer] FATAL ERROR: {e}\n")
    finally:
        with collector_lock:
            importer_state["running"] = False
            importer_state["last_finished_at"] = time.time()
            importer_state["instance"] = None


def _run_json_thread():
    """
    اجرای job آپدیتر JSON در بک‌گراند.
    """
    log_stream = CollectorLogStream()

    with collector_lock:
        json_state["running"] = True
        json_state["last_started_at"] = time.time()
        json_state["last_finished_at"] = None
        json_state["stats"] = None
        json_state["instance"] = None
        collector_state["log"].clear()

    with contextlib.redirect_stdout(log_stream):
        print("[json_updater] starting JSON updater background thread...")

        try:
            updater = JsonConfigUpdater(batch_size=1000)
            print("[json_updater] JsonConfigUpdater initialized.")
            with collector_lock:
                json_state["instance"] = updater
        except Exception as e:
            print(f"[json_updater] FATAL ERROR during init: {e}")
            with collector_lock:
                json_state["running"] = False
                json_state["last_finished_at"] = time.time()
                json_state["instance"] = None
            return

        try:
            updater.update_missing_json()
            with collector_lock:
                json_state["stats"] = {"json_stats": updater.stats}
        except Exception as e:
            print(f"[json_updater] FATAL ERROR inside job: {e}")
        finally:
            with collector_lock:
                json_state["running"] = False
                json_state["last_finished_at"] = time.time()
                json_state["instance"] = None
            print("[json_updater] background thread finished.")


def _run_hash_thread():
    """
    اجرای job آپدیتر hash (config_hash) در بک‌گراند.
    """
    log_stream = CollectorLogStream()

    with collector_lock:
        hash_state["running"] = True
        hash_state["last_started_at"] = time.time()
        hash_state["last_finished_at"] = None
        hash_state["stats"] = None
        hash_state["instance"] = None
        collector_state["log"].clear()

    with contextlib.redirect_stdout(log_stream):
        print("[hash_updater] starting hash updater background thread...")

        try:
            updater = ConfigHashUpdater(batch_size=1000)
            print("[hash_updater] ConfigHashUpdater initialized.")
            with collector_lock:
                hash_state["instance"] = updater
        except Exception as e:
            print(f"[hash_updater] FATAL ERROR during init: {e}")
            with collector_lock:
                hash_state["running"] = False
                hash_state["last_finished_at"] = time.time()
                hash_state["instance"] = None
            return

        try:
            updater.update_hashes()
            with collector_lock:
                hash_state["stats"] = {"hash_stats": updater.stats}
        except Exception as e:
            print(f"[hash_updater] FATAL ERROR inside job: {e}")
        finally:
            with collector_lock:
                hash_state["running"] = False
                hash_state["last_finished_at"] = time.time()
                hash_state["instance"] = None
            print("[hash_updater] background thread finished.")


def _run_compress_thread():
    """
    اجرای job بکاپ فشرده‌ی دیتابیس در بک‌گراند.
    از اسکریپت xraymgr.compress_db استفاده می‌کند و لاگ را در پنل نشان می‌دهد.
    """
    log_stream = CollectorLogStream()

    with collector_lock:
        compress_state["running"] = True
        compress_state["last_started_at"] = time.time()
        compress_state["last_finished_at"] = None
        compress_state["stats"] = None
        compress_state["instance"] = None
        collector_state["log"].clear()

    with contextlib.redirect_stdout(log_stream):
        print("[compress] starting database compression background thread...")

        try:
            db_path = get_db_path()
            db_path = os.path.abspath(db_path)
            db_dir = os.path.dirname(db_path)
            db_base = os.path.basename(db_path)
            ts = time.strftime("%Y%m%d-%H%M%S")
            out_name = f"{db_base}.{ts}.xz"
            out_path = os.path.join(db_dir, out_name)
            print(f"[compress] expected output file: {out_path}")

            # اجرای main اسکریپت که خودش فایل خروجی را می‌سازد
            compress_db_main()

            with collector_lock:
                compress_state["stats"] = {
                    "compress_stats": {
                        "output_file": out_path
                    }
                }
        except Exception as e:
            print(f"[compress] FATAL ERROR inside job: {e}")
        finally:
            with collector_lock:
                compress_state["running"] = False
                compress_state["last_finished_at"] = time.time()
                compress_state["instance"] = None
            print("[compress] background thread finished.")


# ----------------- Pydantic models -----------------


class SQLQuery(BaseModel):
    query: str
    params: Optional[List[Any]] = None


# ----------------- Routes: UI (HTML) -----------------


@app.get("/", response_class=HTMLResponse)
async def index():
    index_path = STATIC_DIR / "index.html"
    try:
        html = index_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return HTMLResponse(content="index.html not found", status_code=500)
    return HTMLResponse(content=html)


# ----------------- Routes: API -----------------


@app.get("/health")
async def health():
    db_path = get_db_path()
    ok = os.path.exists(db_path)
    return {"status": "ok" if ok else "missing", "db_path": db_path}


@app.get("/db/tables")
async def get_tables():
    # هنوز API هست، شاید بعداً به درد بخورد؛ UI فعلاً ازش استفاده نمی‌کند.
    return JSONResponse(list_tables())


@app.get("/db/table/{table_name}")
async def get_table(table_name: str, limit: int = 100):
    try:
        data = fetch_table(table_name, limit=limit)
        return JSONResponse(data)
    except sqlite3.Error as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/db/query")
async def post_query(payload: SQLQuery):
    q = payload.query
    if not q.strip():
        raise HTTPException(status_code=400, detail="Empty query.")
    try:
        data = run_query(q, payload.params)
        return JSONResponse(data)
    except sqlite3.Error as e:
        raise HTTPException(status_code=400, detail=str(e))


# ---- Collector endpoints ----


@app.post("/collector/run")
async def run_collector():
    with collector_lock:
        if collector_state["running"]:
            raise HTTPException(status_code=409, detail="Collector is already running")
        t = threading.Thread(target=_run_collector_thread, daemon=True)
        t.start()
    return {"status": "started"}


@app.post("/collector/stop")
async def stop_collector():
    with collector_lock:
        if not collector_state["running"]:
            raise HTTPException(status_code=409, detail="Collector is not running")
        inst = collector_state.get("instance")
    if inst is None:
        raise HTTPException(status_code=500, detail="Collector instance not available")
    inst.request_stop()
    return {"status": "stopping"}


@app.get("/collector/status")
async def collector_status():
    with collector_lock:
        return {
            "running": collector_state["running"],
            "last_started_at": collector_state["last_started_at"],
            "last_finished_at": collector_state["last_finished_at"],
            "stats": collector_state["stats"],
            "log_length": len(job_log_buffer),
        }


@app.get("/collector/log")
async def collector_log(offset: int = Query(0, ge=0)):
    """
    برای سازگاری با نسخه‌های قدیمی UI:
    - از job_log_buffer می‌خوانیم (حداکثر ۵۰ خط)
    - offset همچنان پشتیبانی می‌شود، هرچند UI جدید دیگر از آن استفاده نمی‌کند.
    """
    with collector_lock:
        lines = list(job_log_buffer)
        total = len(lines)
        if offset < 0 or offset > total:
            offset = 0
        sliced = lines[offset:]
    return {"offset": offset, "total": total, "lines": sliced}


# ---- Importer endpoints ----


@app.post("/importer/run")
async def run_importer():
    with collector_lock:
        if importer_state["running"]:
            raise HTTPException(status_code=409, detail="Importer is already running")
        t = threading.Thread(target=_run_importer_thread, daemon=True)
        t.start()
    return {"status": "started"}


@app.post("/importer/stop")
async def stop_importer():
    with collector_lock:
        if not importer_state["running"]:
            raise HTTPException(status_code=409, detail="Importer is not running")
        inst = importer_state.get("instance")
    if inst is None:
        raise HTTPException(status_code=500, detail="Importer instance not available")
    inst.request_stop()
    return {"status": "stopping"}


@app.get("/importer/status")
async def importer_status():
    with collector_lock:
        return {
            "running": importer_state["running"],
            "last_started_at": importer_state["last_started_at"],
            "last_finished_at": importer_state["last_finished_at"],
            "stats": importer_state["stats"],
        }


# ---- JSON updater endpoints ----


@app.post("/json/run")
async def run_json():
    with collector_lock:
        if json_state["running"]:
            raise HTTPException(status_code=409, detail="JSON updater is already running")
        t = threading.Thread(target=_run_json_thread, daemon=True)
        t.start()
    return {"status": "started"}


@app.post("/json/stop")
async def stop_json():
    with collector_lock:
        if not json_state["running"]:
            raise HTTPException(status_code=409, detail="JSON updater is not running")
        inst = json_state.get("instance")
    if inst is None:
        raise HTTPException(status_code=500, detail="JSON updater instance not available")
    inst.request_stop()
    return {"status": "stopping"}


@app.get("/json/status")
async def json_status():
    with collector_lock:
        return {
            "running": json_state["running"],
            "last_started_at": json_state["last_started_at"],
            "last_finished_at": json_state["last_finished_at"],
            "stats": json_state["stats"],
        }


# ---- Hash updater endpoints ----


@app.post("/hash/run")
async def run_hash():
    with collector_lock:
        if hash_state["running"]:
            raise HTTPException(status_code=409, detail="Hash updater is already running")
        t = threading.Thread(target=_run_hash_thread, daemon=True)
        t.start()
    return {"status": "started"}


@app.post("/hash/stop")
async def stop_hash():
    with collector_lock:
        if not hash_state["running"]:
            raise HTTPException(status_code=409, detail="Hash updater is not running")
        inst = hash_state.get("instance")
    if inst is None:
        raise HTTPException(status_code=500, detail="Hash updater instance not available")
    inst.request_stop()
    return {"status": "stopping"}


@app.get("/hash/status")
async def hash_status():
    with collector_lock:
        return {
            "running": hash_state["running"],
            "last_started_at": hash_state["last_started_at"],
            "last_finished_at": hash_state["last_finished_at"],
            "stats": hash_state["stats"],
        }


# ---- Compress (DB backup) endpoints ----


@app.post("/compress/run")
async def run_compress():
    with collector_lock:
        if compress_state["running"]:
            raise HTTPException(status_code=409, detail="Compress job is already running")
        t = threading.Thread(target=_run_compress_thread, daemon=True)
        t.start()
    return {"status": "started"}


@app.get("/compress/status")
async def compress_status():
    with collector_lock:
        return {
            "running": compress_state["running"],
            "last_started_at": compress_state["last_started_at"],
            "last_finished_at": compress_state["last_finished_at"],
            "stats": compress_state["stats"],
        }


# ---- Jobs summary endpoint (برای UI جدید) ----


@app.get("/jobs/summary")
async def jobs_summary():
    """
    یک شات کلی برای پنل:
      - مسیر دیتابیس
      - وضعیت همهٔ jobها
      - آخرین ۵۰ خط لاگ مشترک
    """
    db_path = get_db_path()
    with collector_lock:
        log_lines = list(job_log_buffer)
        jobs = {
            "collector": {
                "running": collector_state["running"],
                "last_started_at": collector_state["last_started_at"],
                "last_finished_at": collector_state["last_finished_at"],
                "stats": collector_state["stats"],
            },
            "importer": {
                "running": importer_state["running"],
                "last_started_at": importer_state["last_started_at"],
                "last_finished_at": importer_state["last_finished_at"],
                "stats": importer_state["stats"],
            },
            "json": {
                "running": json_state["running"],
                "last_started_at": json_state["last_started_at"],
                "last_finished_at": json_state["last_finished_at"],
                "stats": json_state["stats"],
            },
            "hash": {
                "running": hash_state["running"],
                "last_started_at": hash_state["last_started_at"],
                "last_finished_at": hash_state["last_finished_at"],
                "stats": hash_state["stats"],
            },
            "compress": {
                "running": compress_state["running"],
                "last_started_at": compress_state["last_started_at"],
                "last_finished_at": compress_state["last_finished_at"],
                "stats": compress_state["stats"],
            },
        }

    return {
        "db_path": db_path,
        "jobs": jobs,
        "log": {
            "lines": log_lines,
            "total": len(log_lines),
        },
    }


def get_app():
    return app
