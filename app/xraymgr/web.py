from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
from typing import List, Optional, Dict, Any

import sqlite3
import os
import threading
import time
import sys
import contextlib

from .settings import get_db_path
from .collector import SubscriptionCollector
from .importer import RawConfigImporter
from .json_updater import JsonConfigUpdater

app = FastAPI(title="XrayMgr Web Dashboard")


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


collector_state = {
    "running": False,
    "last_started_at": None,
    "last_finished_at": None,
    "stats": None,      # dict یا None
    "log": [],          # list[str] - لاگ مشترک
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

collector_lock = threading.Lock()


class CollectorLogStream:
    """
    استریم ساده برای گرفتن خروجی printهای jobها (کالکتور/ایمپورتر/json_updater):
      - هم‌زمان روی stdout واقعی می‌نویسد
      - علاوه بر آن، لاگ‌ها را در collector_state["log"] ذخیره می‌کند
    """

    def write(self, s: str) -> int:
        if not s:
            return 0
        sys.__stdout__.write(s)
        lines = s.splitlines()
        with collector_lock:
            for line in lines:
                if line.strip():
                    collector_state["log"].append(line)
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


# ----------------- Pydantic models -----------------


class SQLQuery(BaseModel):
    query: str
    params: Optional[List[Any]] = None


# ----------------- Routes: UI (HTML) -----------------


@app.get("/", response_class=HTMLResponse)
async def index():
    html = r"""
<!DOCTYPE html>
<html lang="fa">
<head>
    <meta charset="UTF-8">
    <title>XrayMgr Dashboard</title>
    <style>
        body {
            font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
            margin: 0;
            padding: 0;
            background: #0f172a;
            color: #e5e7eb;
            direction: rtl;
        }
        header {
            background: #111827;
            padding: 16px 24px;
            border-bottom: 1px solid #1f2937;
            display: flex;
            align-items: baseline;
            justify-content: space-between;
            gap: 16px;
        }
        header h1 {
            margin: 0;
            font-size: 20px;
            color: #f9fafb;
        }
        header span.subtitle {
            color: #9ca3af;
            font-size: 13px;
        }
        main {
            padding: 16px 24px 32px 24px;
        }
        .card {
            background: #020617;
            border-radius: 10px;
            padding: 14px 16px 16px;
            border: 1px solid #1f2937;
            box-shadow: 0 10px 25px rgba(0,0,0,0.45);
        }
        .card h2 {
            margin: 0 0 10px 0;
            font-size: 16px;
            color: #e5e7eb;
        }
        .card small {
            color: #6b7280;
        }
        .card-header {
            display: flex;
            justify-content: space-between;
            align-items: baseline;
            gap: 8px;
            margin-bottom: 8px;
        }
        .status-ok {
            color: #22c55e;
            font-size: 13px;
        }
        .status-bad {
            color: #ef4444;
            font-size: 13px;
        }
        select, input, textarea, button {
            font-family: inherit;
            font-size: 13px;
        }
        select, input, textarea {
            background: #020617;
            border-radius: 6px;
            border: 1px solid #374151;
            color: #e5e7eb;
            padding: 6px 8px;
            width: 100%;
            box-sizing: border-box;
        }
        select:focus, input:focus, textarea:focus {
            outline: 2px solid #2563eb;
            outline-offset: 1px;
            border-color: #2563eb;
        }
        textarea {
            resize: vertical;
            min-height: 90px;
        }
        button {
            border-radius: 6px;
            border: none;
            padding: 6px 10px;
            cursor: pointer;
            font-weight: 500;
            background: #2563eb;
            color: #e5e7eb;
            display: inline-flex;
            align-items: center;
            gap: 6px;
        }
        button.secondary {
            background: #111827;
            border: 1px solid #374151;
        }
        button.danger {
            background: #b91c1c;
        }
        button:disabled {
            opacity: 0.5;
            cursor: default;
        }
        table {
            border-collapse: collapse;
            width: 100%;
            font-size: 12px;
            direction: ltr;
        }
        th, td {
            border-bottom: 1px solid #1f2937;
            padding: 4px 6px;
            text-align: left;
        }
        th {
            background: #020617;
            color: #9ca3af;
            position: sticky;
            top: 0;
            z-index: 1;
        }
        tr:nth-child(even) td {
            background: rgba(15,23,42,0.6);
        }
        tr:hover td {
            background: rgba(30,64,175,0.25);
        }
        .table-container {
            max-height: 420px;
            overflow: auto;
            border-radius: 8px;
            border: 1px solid #111827;
            background: radial-gradient(circle at top, rgba(37,99,235,0.08), transparent 55%);
        }
        code {
            font-family: "JetBrains Mono", ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
            font-size: 12px;
        }
        pre {
            font-family: "JetBrains Mono", ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
            font-size: 12px;
            background: #020617;
            border-radius: 8px;
            border: 1px solid #1f2937;
            padding: 8px 10px;
            max-height: 360px;
            min-height: 220px;
            overflow: auto;
            white-space: pre-wrap;
        }
        .muted {
            color: #6b7280;
            font-size: 12px;
        }
        .pill {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            padding: 2px 8px;
            border-radius: 999px;
            font-size: 11px;
            background: rgba(15,118,110,0.1);
            color: #14b8a6;
        }
        .pill span.dot {
            width: 8px;
            height: 8px;
            border-radius: 999px;
            background: #22c55e;
        }
        .pill.red {
            background: rgba(185,28,28,0.2);
            color: #f97316;
        }
        .pill.red span.dot {
            background: #f97316;
        }
        .pill.gray {
            background: rgba(75,85,99,0.4);
            color: #e5e7eb;
        }
        footer {
            margin-top: 16px;
            font-size: 11px;
            color: #6b7280;
        }
        .btn-row {
            display: flex;
            gap: 8px;
            flex-wrap: wrap;
            margin-top: 4px;
            margin-bottom: 8px;
        }
        .label-row {
            display: flex;
            justify-content: space-between;
            font-size: 11px;
            color: #9ca3af;
            margin-bottom: 4px;
        }
        .status-line {
            font-size: 12px;
            color: #9ca3af;
            margin-top: 4px;
        }
        hr {
            border: 0;
            border-top: 1px solid #111827;
            margin: 10px 0;
        }

        /* تب‌ها */

        .tabs {
            display: flex;
            gap: 8px;
            margin-bottom: 16px;
            border-bottom: 1px solid #1f2937;
        }
        .tab-button {
            border: none;
            background: transparent;
            padding: 8px 12px;
            cursor: pointer;
            font-size: 13px;
            border-radius: 8px 8px 0 0;
            color: #9ca3af;
        }
        .tab-button:hover {
            background: rgba(15,23,42,0.8);
        }
        .tab-button.active {
            background: #020617;
            color: #e5e7eb;
            border: 1px solid #1f2937;
            border-bottom-color: #020617;
        }
        .tab-content {
            display: none;
        }
        .tab-content.active {
            display: block;
        }

        /* layout برای تب jobها */

        .jobs-grid {
            display: grid;
            grid-template-columns: minmax(0, 1.6fr) minmax(0, 2.2fr);
            gap: 12px;
        }

        @media (max-width: 900px) {
            .jobs-grid {
                grid-template-columns: minmax(0, 1fr);
            }
        }
    </style>
</head>
<body>
<header>
    <div>
        <h1>XrayMgr Dashboard</h1>
        <span class="subtitle">وضعیت Jobها، دیتابیس و لاگ‌ها</span>
    </div>
    <div class="pill gray">
        <span class="dot"></span>
        <span>SQLite: <code id="db-path">...</code></span>
    </div>
</header>
<main>
    <div class="tabs">
        <button class="tab-button active" data-tab="jobs" onclick="switchTab('jobs')">
            Jobها / پردازش‌ها
        </button>
        <button class="tab-button" data-tab="db" onclick="switchTab('db')">
            دیتابیس / SQL
        </button>
    </div>

    <!-- تب Jobها -->

    <div id="tab-jobs" class="tab-content active">
        <section class="card">
            <div class="card-header">
                <h2>Jobهای بک‌گراند</h2>
                <small>کالکتور ساب‌ها + ایمپورتر raw_configs → links + آپدیتر JSON</small>
            </div>

            <div class="jobs-grid">
                <div class="jobs-controls">
                    <!-- Collector controls -->
                    <div class="label-row">
                        <span>وضعیت کالکتور ساب‌ها:</span>
                        <span id="collector-status" class="muted">...</span>
                    </div>
                    <div class="btn-row">
                        <button id="collector-run-btn" onclick="startCollector()">شروع کالکتور</button>
                        <button class="danger" id="collector-stop-btn" onclick="stopCollector()" disabled>توقف کالکتور</button>
                        <button class="secondary" onclick="refreshCollectorStatus()">رفرش وضعیت</button>
                    </div>
                    <div id="collector-stats" class="status-line"></div>

                    <hr>

                    <!-- Importer controls -->
                    <div class="label-row">
                        <span>وضعیت ایمپورتر raw_configs → links:</span>
                        <span id="importer-status" class="muted">...</span>
                    </div>
                    <div class="btn-row">
                        <button id="importer-run-btn" onclick="startImporter()">شروع ایمپورتر</button>
                        <button class="danger" id="importer-stop-btn" onclick="stopImporter()" disabled>توقف ایمپورتر</button>
                        <button class="secondary" onclick="refreshImporterStatus()">رفرش وضعیت</button>
                    </div>
                    <div id="importer-stats" class="status-line"></div>

                    <hr>

                    <!-- JSON updater controls -->
                    <div class="label-row">
                        <span>وضعیت آپدیتر JSON (links.config_json):</span>
                        <span id="json-status" class="muted">...</span>
                    </div>
                    <div class="btn-row">
                        <button id="json-run-btn" onclick="startJson()">شروع JSON updater</button>
                        <button class="danger" id="json-stop-btn" onclick="stopJson()" disabled>توقف JSON updater</button>
                        <button class="secondary" onclick="refreshJsonStatus()">رفرش وضعیت</button>
                    </div>
                    <div id="json-stats" class="status-line"></div>
                </div>

                <div class="jobs-log">
                    <div class="label-row" style="margin-top: 4px;">
                        <span>لاگ jobهای بک‌گراند:</span>
                        <span class="muted">خروجی آخرین اجرا (با auto-refresh تا وقتی jobها در حال اجرا هستند)</span>
                    </div>
                    <pre id="collector-log">(هنوز لاگی ثبت نشده است)</pre>
                </div>
            </div>
        </section>
    </div>

    <!-- تب دیتابیس / SQL -->

    <div id="tab-db" class="tab-content">
        <section class="card">
            <div class="card-header">
                <h2>دیتابیس و کنسول SQL</h2>
                <small>فقط کوئری بنویس، بزن اجرا؛ بقیه‌اش با ما.</small>
            </div>

            <div>
                <div class="label-row">
                    <span>کوئری SQL</span>
                    <span class="muted">احتیاط: اینجا هر دستور SQLite (SELECT / UPDATE / DELETE / DDL) اجرا می‌شود.</span>
                </div>
                <textarea id="sql-input" placeholder="هر دستور معتبر SQLite را می‌توانی اینجا اجرا کنی"></textarea>
                <div class="btn-row">
                    <button onclick="runQuery()">اجرای کوئری</button>
                    <select id="sql-presets" onchange="applyPresetQuery()">
                        <option value="">کوئری آماده…</option>
                        <option value="SELECT * FROM links LIMIT 10;">۱۰ سطر اول links</option>
                    </select>
                </div>
                <div id="sql-status" class="status-line"></div>
            </div>

            <div class="label-row" style="margin-top: 10px;">
                <span>نتیجهٔ کوئری:</span>
                <span class="muted">برای خوانایی، فقط بخشی از نتیجه نمایش داده می‌شود.</span>
            </div>
            <div id="sql-result" class="table-container" style="margin-top: 8px;">
                <div class="muted">نتیجه‌ای برای نمایش نیست.</div>
            </div>
        </section>
    </div>

    <footer>
        مسیر دیتابیس: <code id="footer-db-path">...</code>
    </footer>
</main>

<script>
    const dbPathEl = document.getElementById("db-path");
    const footerDbPathEl = document.getElementById("footer-db-path");

    const sqlInput = document.getElementById("sql-input");
    const sqlStatus = document.getElementById("sql-status");
    const sqlResult = document.getElementById("sql-result");
    const sqlPresets = document.getElementById("sql-presets");

    const collectorStatusEl = document.getElementById("collector-status");
    const collectorStatsEl = document.getElementById("collector-stats");
    const collectorLogEl = document.getElementById("collector-log");
    const collectorRunBtn = document.getElementById("collector-run-btn");
    const collectorStopBtn = document.getElementById("collector-stop-btn");

    const importerStatusEl = document.getElementById("importer-status");
    const importerStatsEl = document.getElementById("importer-stats");
    const importerRunBtn = document.getElementById("importer-run-btn");
    const importerStopBtn = document.getElementById("importer-stop-btn");

    const jsonStatusEl = document.getElementById("json-status");
    const jsonStatsEl = document.getElementById("json-stats");
    const jsonRunBtn = document.getElementById("json-run-btn");
    const jsonStopBtn = document.getElementById("json-stop-btn");

    const tabButtons = document.querySelectorAll(".tab-button");
    const tabContents = document.querySelectorAll(".tab-content");

    let collectorLogOffset = 0;
    let collectorRunning = false;
    let collectorPolling = false;
    let importerRunning = false;
    let jsonRunning = false;

    function switchTab(target) {
        tabButtons.forEach((btn) => {
            btn.classList.toggle("active", btn.dataset.tab === target);
        });
        tabContents.forEach((tab) => {
            tab.classList.toggle("active", tab.id === "tab-" + target);
        });
    }

    async function refreshHealth() {
        try {
            const res = await fetch("/health");
            if (!res.ok) throw new Error("HTTP " + res.status);
            const data = await res.json();
            if (dbPathEl) dbPathEl.textContent = data.db_path || "unknown";
            if (footerDbPathEl) footerDbPathEl.textContent = data.db_path || "unknown";
        } catch (err) {
            // اگر چیزی خراب باشد، همین که db-path خالی نیست برای ما کافی است
        }
    }

    function renderTable(data, container) {
        const cols = data.columns || [];
        const rows = data.rows || [];
        if (cols.length === 0) {
            container.innerHTML = '<div class="muted">هیچ ستونی برای نمایش نیست.</div>';
            return;
        }
        let html = '<table><thead><tr>';
        cols.forEach(c => {
            html += `<th>${escapeHtml(c)}</th>`;
        });
        html += '</tr></thead><tbody>';
        if (rows.length === 0) {
            html += '<tr><td colspan="' + cols.length + '" class="muted">داده‌ای وجود ندارد.</td></tr>';
        } else {
            rows.forEach(row => {
                html += '<tr>';
                cols.forEach(c => {
                    let v = row[c];
                    if (v === null || v === undefined) v = "";
                    html += `<td>${escapeHtml(String(v))}</td>`;
                });
                html += '</tr>';
            });
        }
        html += '</tbody></table>';
        container.innerHTML = html;
    }

    function escapeHtml(str) {
        return str
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;");
    }

    async function runQuery() {
        if (!sqlInput || !sqlResult || !sqlStatus) return;
        const q = sqlInput.value;
        sqlStatus.textContent = "";
        sqlResult.innerHTML = '<div class="muted">...</div>';
        if (!q.trim()) {
            sqlStatus.textContent = "کوئری خالی است.";
            return;
        }
        try {
            const res = await fetch("/db/query", {
                method: "POST",
                headers: {"Content-Type": "application/json"},
                body: JSON.stringify({query: q, params: []})
            });
            if (!res.ok) {
                const text = await res.text();
                throw new Error(text || ("HTTP " + res.status));
            }
            const data = await res.json();
            if (data.columns && data.columns.length > 0) {
                renderTable(data, sqlResult);
                sqlStatus.textContent = `نتیجه: ${data.rows.length} ردیف`;
            } else {
                sqlResult.innerHTML = '<div class="muted">کوئری بدون نتیجهٔ جدولی اجرا شد.</div>';
                sqlStatus.textContent = `rowcount: ${data.rowcount}`;
            }
        } catch (err) {
            console.error(err);
            sqlStatus.textContent = "خطا در اجرای کوئری: " + err;
            sqlResult.innerHTML = '<div class="muted">خطا در اجرای کوئری.</div>';
        }
    }

    function applyPresetQuery() {
        if (!sqlPresets || !sqlInput) return;
        const q = sqlPresets.value;
        if (!q) return;
        sqlInput.value = q;
        runQuery();
    }

    // ---- Collector controls ----

    async function refreshCollectorStatus() {
        try {
            const res = await fetch("/collector/status");
            if (!res.ok) throw new Error("HTTP " + res.status);
            const data = await res.json();

            collectorRunning = !!data.running;
            const runningLabel = collectorRunning ? "در حال اجرا" : "متوقف";
            const pillClass = collectorRunning ? "pill" : "pill gray";
            collectorStatusEl.innerHTML =
                `<span class="${pillClass}"><span class="dot"></span><span>${runningLabel}</span></span>`;

            collectorRunBtn.disabled = !!collectorRunning;
            collectorStopBtn.disabled = !collectorRunning;

            if (data.stats && data.stats.collector_stats) {
                const s = data.stats.collector_stats;
                collectorStatsEl.textContent =
                    `سورس‌ها: ${s.total_sources || 0} | موفق: ${s.successful_sources || 0} | خراب: ${s.failed_sources || 0} | کانفیگ جمع‌شده: ${s.total_configs || 0}`;
            } else {
                collectorStatsEl.textContent = "";
            }

            if ((collectorRunning || importerRunning || jsonRunning) && !collectorPolling) {
                collectorPolling = true;
                pollCollectorLog();
            }
        } catch (err) {
            console.error(err);
            collectorStatusEl.innerHTML =
                '<span class="pill red"><span class="dot"></span><span>خطا در وضعیت کالکتور</span></span>';
        }
    }

    async function startCollector() {
        try {
            collectorRunBtn.disabled = true;
            collectorStopBtn.disabled = false;
            collectorLogOffset = 0;
            collectorLogEl.textContent = "";
            const res = await fetch("/collector/run", {method: "POST"});
            if (!res.ok) {
                const text = await res.text();
                throw new Error(text || ("HTTP " + res.status));
            }
            collectorStatusEl.innerHTML =
                '<span class="pill"><span class="dot"></span><span>در حال اجرا...</span></span>';
            collectorRunning = true;
            if (!collectorPolling) {
                collectorPolling = true;
                pollCollectorLog();
            }
        } catch (err) {
            console.error(err);
            collectorRunBtn.disabled = false;
            collectorStopBtn.disabled = true;
            collectorStatusEl.innerHTML =
                '<span class="pill red"><span class="dot"></span><span>خطا در شروع کالکتور</span></span>';
        }
    }

    async function stopCollector() {
        try {
            collectorStopBtn.disabled = true;
            const res = await fetch("/collector/stop", {method: "POST"});
            if (!res.ok) {
                const text = await res.text();
                throw new Error(text || ("HTTP " + res.status));
            }
            collectorStatusEl.innerHTML =
                '<span class="pill red"><span class="dot"></span><span>در حال توقف...</span></span>';
        } catch (err) {
            console.error(err);
            collectorStatusEl.innerHTML =
                '<span class="pill red"><span class="dot"></span><span>خطا در توقف کالکتور</span></span>';
        }
    }

    // ---- Importer controls ----

    async function refreshImporterStatus() {
        try {
            const res = await fetch("/importer/status");
            if (!res.ok) throw new Error("HTTP " + res.status);
            const data = await res.json();

            importerRunning = !!data.running;
            const runningLabel = importerRunning ? "در حال اجرا" : "متوقف";
            const pillClass = importerRunning ? "pill" : "pill gray";
            importerStatusEl.innerHTML =
                `<span class="${pillClass}"><span class="dot"></span><span>${runningLabel}</span></span>`;

            importerRunBtn.disabled = !!importerRunning;
            importerStopBtn.disabled = !importerRunning;

            if (data.stats && data.stats.importer_stats) {
                const s = data.stats.importer_stats;
                importerStatsEl.textContent =
                    `خطوط: ${s.total_lines || 0} | کانفیگ معتبر: ${s.valid_configs || 0} | جدید درج‌شده: ${s.inserted || 0} | batchها: ${s.batches_committed || 0}`;
            } else {
                importerStatsEl.textContent = "";
            }

            if ((collectorRunning || importerRunning || jsonRunning) && !collectorPolling) {
                collectorPolling = true;
                pollCollectorLog();
            }
        } catch (err) {
            console.error(err);
            importerStatusEl.innerHTML =
                '<span class="pill red"><span class="dot"></span><span>خطا در وضعیت ایمپورتر</span></span>';
        }
    }

    async function startImporter() {
        try {
            importerRunBtn.disabled = true;
            importerStopBtn.disabled = false;
            collectorLogOffset = 0;
            collectorLogEl.textContent = "";
            const res = await fetch("/importer/run", {method: "POST"});
            if (!res.ok) {
                const text = await res.text();
                throw new Error(text || ("HTTP " + res.status));
            }
            importerStatusEl.innerHTML =
                '<span class="pill"><span class="dot"></span><span>در حال اجرا...</span></span>';
            importerRunning = true;
            if (!collectorPolling) {
                collectorPolling = true;
                pollCollectorLog();
            }
        } catch (err) {
            console.error(err);
            importerRunBtn.disabled = false;
            importerStopBtn.disabled = true;
            importerStatusEl.innerHTML =
                '<span class="pill red"><span class="dot"></span><span>خطا در شروع ایمپورتر</span></span>';
        }
    }

    async function stopImporter() {
        try {
            importerStopBtn.disabled = true;
            const res = await fetch("/importer/stop", {method: "POST"});
            if (!res.ok) {
                const text = await res.text();
                throw new Error(text || ("HTTP " + res.status));
            }
            importerStatusEl.innerHTML =
                '<span class="pill red"><span class="dot"></span><span>در حال توقف...</span></span>';
        } catch (err) {
            console.error(err);
            importerStatusEl.innerHTML =
                '<span class="pill red"><span class="dot"></span><span>خطا در توقف ایمپورتر</span></span>';
        }
    }

    // ---- JSON updater controls ----

    async function refreshJsonStatus() {
        try {
            const res = await fetch("/json/status");
            if (!res.ok) throw new Error("HTTP " + res.status);
            const data = await res.json();

            jsonRunning = !!data.running;
            const runningLabel = jsonRunning ? "در حال اجرا" : "متوقف";
            const pillClass = jsonRunning ? "pill" : "pill gray";
            jsonStatusEl.innerHTML =
                `<span class="${pillClass}"><span class="dot"></span><span>${runningLabel}</span></span>`;

            jsonRunBtn.disabled = !!jsonRunning;
            jsonStopBtn.disabled = !jsonRunning;

            if (data.stats && data.stats.json_stats) {
                const s = data.stats.json_stats;
                jsonStatsEl.textContent =
                    `کاندید: ${s.total_candidates || 0} | batchها: ${s.batches || 0} | URLها: ${s.urls_seen || 0} | موفق: ${s.urls_converted || 0} | خراب: ${s.urls_failed || 0} | ردیف آپدیت‌شده: ${s.rows_updated || 0}`;
            } else {
                jsonStatsEl.textContent = "";
            }

            if ((collectorRunning || importerRunning || jsonRunning) && !collectorPolling) {
                collectorPolling = true;
                pollCollectorLog();
            }
        } catch (err) {
            console.error(err);
            jsonStatusEl.innerHTML =
                '<span class="pill red"><span class="dot"></span><span>خطا در وضعیت JSON updater</span></span>';
        }
    }

    async function startJson() {
        try {
            jsonRunBtn.disabled = true;
            jsonStopBtn.disabled = false;
            collectorLogOffset = 0;
            collectorLogEl.textContent = "";
            const res = await fetch("/json/run", {method: "POST"});
            if (!res.ok) {
                const text = await res.text();
                throw new Error(text || ("HTTP " + res.status));
            }
            jsonStatusEl.innerHTML =
                '<span class="pill"><span class="dot"></span><span>در حال اجرا...</span></span>';
            jsonRunning = true;
            if (!collectorPolling) {
                collectorPolling = true;
                pollCollectorLog();
            }
        } catch (err) {
            console.error(err);
            jsonRunBtn.disabled = false;
            jsonStopBtn.disabled = true;
            jsonStatusEl.innerHTML =
                '<span class="pill red"><span class="dot"></span><span>خطا در شروع JSON updater</span></span>';
        }
    }

    async function stopJson() {
        try {
            jsonStopBtn.disabled = true;
            const res = await fetch("/json/stop", {method: "POST"});
            if (!res.ok) {
                const text = await res.text();
                throw new Error(text || ("HTTP " + res.status));
            }
            jsonStatusEl.innerHTML =
                '<span class="pill red"><span class="dot"></span><span>در حال توقف...</span></span>';
        } catch (err) {
            console.error(err);
            jsonStatusEl.innerHTML =
                '<span class="pill red"><span class="dot"></span><span>خطا در توقف JSON updater</span></span>';
        }
    }

    // ---- Shared log polling ----

    async function pollCollectorLog() {
        try {
            const res = await fetch("/collector/log?offset=" + collectorLogOffset);
            if (res.ok) {
                const data = await res.json();
                const lines = data.lines || [];
                if (lines.length > 0) {
                    const textToAdd = lines.join("\n");
                    if (collectorLogEl.textContent === "(هنوز لاگی ثبت نشده است)" || collectorLogOffset === 0) {
                        collectorLogEl.textContent = textToAdd;
                    } else {
                        collectorLogEl.textContent += (collectorLogEl.textContent ? "\n" : "") + textToAdd;
                    }
                    collectorLogEl.scrollTop = collectorLogEl.scrollHeight;
                    collectorLogOffset = data.total;
                }
            }
        } catch (err) {
            console.error("pollCollectorLog error", err);
        } finally {
            if (collectorRunning || importerRunning || jsonRunning) {
                setTimeout(pollCollectorLog, 2000);
            } else {
                collectorPolling = false;
            }
        }
    }

    window.addEventListener("load", () => {
        refreshHealth();
        refreshCollectorStatus();
        refreshImporterStatus();
        refreshJsonStatus();
        setInterval(refreshCollectorStatus, 5000);
        setInterval(refreshImporterStatus, 5000);
        setInterval(refreshJsonStatus, 5000);
    });
</script>
</body>
</html>
    """
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
            "log_length": len(collector_state["log"]),
        }


@app.get("/collector/log")
async def collector_log(offset: int = Query(0, ge=0)):
    with collector_lock:
        total = len(collector_state["log"])
        if offset < 0 or offset > total:
            offset = 0
        lines = collector_state["log"][offset:]
    return {"offset": offset, "total": total, "lines": lines}


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


def get_app():
    return app
