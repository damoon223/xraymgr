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

const hashStatusEl = document.getElementById("hash-status");
const hashStatsEl = document.getElementById("hash-stats");
const hashRunBtn = document.getElementById("hash-run-btn");
const hashStopBtn = document.getElementById("hash-stop-btn");

const compressStatusEl = document.getElementById("compress-status");
const compressStatsEl = document.getElementById("compress-stats");
const compressRunBtn = document.getElementById("compress-run-btn");

const tabButtons = document.querySelectorAll(".tab-button");
const tabContents = document.querySelectorAll(".tab-content");

// وضعیت runtime (در صورت نیاز)
let collectorRunning = false;
let importerRunning = false;
let jsonRunning = false;
let hashRunning = false;
let compressRunning = false;

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

// ---- خلاصهٔ Jobها + لاگ (endpoint /jobs/summary) ----

async function refreshJobsSummary() {
    try {
        const res = await fetch("/jobs/summary");
        if (!res.ok) throw new Error("HTTP " + res.status);
        const data = await res.json();

        // db_path
        if (dbPathEl && data.db_path) dbPathEl.textContent = data.db_path;
        if (footerDbPathEl && data.db_path) footerDbPathEl.textContent = data.db_path;

        const jobs = data.jobs || {};

        // --- collector ---
        const collector = jobs.collector || {};
        collectorRunning = !!collector.running;
        const collectorLabel = collectorRunning ? "در حال اجرا" : "متوقف";
        const collectorClass = collectorRunning ? "pill" : "pill gray";
        if (collectorStatusEl) {
            collectorStatusEl.innerHTML =
                `<span class="${collectorClass}"><span class="dot"></span><span>${collectorLabel}</span></span>`;
        }
        if (collectorRunBtn) collectorRunBtn.disabled = collectorRunning;
        if (collectorStopBtn) collectorStopBtn.disabled = !collectorRunning;

        if (collectorStatsEl) {
            const s = collector.stats && collector.stats.collector_stats;
            if (s) {
                collectorStatsEl.textContent =
                    `سورس‌ها: ${s.total_sources || 0} | موفق: ${s.successful_sources || 0} | خراب: ${s.failed_sources || 0} | کانفیگ جمع‌شده: ${s.total_configs || 0}`;
            } else {
                collectorStatsEl.textContent = "";
            }
        }

        // --- importer ---
        const importer = jobs.importer || {};
        importerRunning = !!importer.running;
        const importerLabel = importerRunning ? "در حال اجرا" : "متوقف";
        const importerClass = importerRunning ? "pill" : "pill gray";
        if (importerStatusEl) {
            importerStatusEl.innerHTML =
                `<span class="${importerClass}"><span class="dot"></span><span>${importerLabel}</span></span>`;
        }
        if (importerRunBtn) importerRunBtn.disabled = importerRunning;
        if (importerStopBtn) importerStopBtn.disabled = !importerRunning;

        if (importerStatsEl) {
            const s = importer.stats && importer.stats.importer_stats;
            if (s) {
                importerStatsEl.textContent =
                    `خطوط: ${s.total_lines || 0} | کانفیگ معتبر: ${s.valid_configs || 0} | جدید درج‌شده: ${s.inserted || 0} | batchها: ${s.batches_committed || 0}`;
            } else {
                importerStatsEl.textContent = "";
            }
        }

        // --- json updater ---
        const jsonJob = jobs.json || {};
        jsonRunning = !!jsonJob.running;
        const jsonLabel = jsonRunning ? "در حال اجرا" : "متوقف";
        const jsonClass = jsonRunning ? "pill" : "pill gray";
        if (jsonStatusEl) {
            jsonStatusEl.innerHTML =
                `<span class="${jsonClass}"><span class="dot"></span><span>${jsonLabel}</span></span>`;
        }
        if (jsonRunBtn) jsonRunBtn.disabled = jsonRunning;
        if (jsonStopBtn) jsonStopBtn.disabled = !jsonRunning;

        if (jsonStatsEl) {
            const s = jsonJob.stats && jsonJob.stats.json_stats;
            if (s) {
                jsonStatsEl.textContent =
                    `کاندید: ${s.total_candidates || 0} | batchها: ${s.batches || 0} | URLها: ${s.urls_seen || 0} | موفق: ${s.urls_converted || 0} | خراب: ${s.urls_failed || 0} | ردیف آپدیت‌شده: ${s.rows_updated || 0}`;
            } else {
                jsonStatsEl.textContent = "";
            }
        }

        // --- hash updater ---
        const hashJob = jobs.hash || {};
        hashRunning = !!hashJob.running;
        const hashLabel = hashRunning ? "در حال اجرا" : "متوقف";
        const hashClass = hashRunning ? "pill" : "pill gray";
        if (hashStatusEl) {
            hashStatusEl.innerHTML =
                `<span class="${hashClass}"><span class="dot"></span><span>${hashLabel}</span></span>`;
        }
        if (hashRunBtn) hashRunBtn.disabled = hashRunning;
        if (hashStopBtn) hashStopBtn.disabled = !hashRunning;

        if (hashStatsEl) {
            const s = hashJob.stats && hashJob.stats.hash_stats;
            if (s) {
                hashStatsEl.textContent =
                    `کاندید: ${s.total_candidates || 0} | batchها: ${s.batches || 0} | پردازش‌شده: ${s.rows_processed || 0} | هش‌شده: ${s.rows_hashed || 0} | ردشده (non-vmess): ${s.rows_skipped_non_vmess || 0} | JSON خراب: ${s.json_decode_errors || 0} | خطای هویت: ${s.identity_errors || 0} | invalid شده: ${s.marked_invalid || 0}`;
            } else {
                hashStatsEl.textContent = "";
            }
        }

        // --- compress (backup) ---
        const compressJob = jobs.compress || {};
        compressRunning = !!compressJob.running;
        const compressLabel = compressRunning ? "در حال اجرا" : "متوقف";
        const compressClass = compressRunning ? "pill" : "pill gray";
        if (compressStatusEl) {
            compressStatusEl.innerHTML =
                `<span class="${compressClass}"><span class="dot"></span><span>${compressLabel}</span></span>`;
        }
        if (compressRunBtn) compressRunBtn.disabled = compressRunning;

        if (compressStatsEl) {
            const s = compressJob.stats && compressJob.stats.compress_stats;
            if (s && s.output_file) {
                compressStatsEl.textContent = `فایل خروجی: ${s.output_file}`;
            } else {
                compressStatsEl.textContent = "";
            }
        }

        // --- لاگ مشترک jobها (فقط آخرین حداکثر ۵۰ خط) ---
        if (collectorLogEl) {
            const log = data.log || {};
            const lines = log.lines || [];
            if (lines.length === 0) {
                collectorLogEl.textContent = "(هنوز لاگی ثبت نشده است)";
            } else {
                collectorLogEl.textContent = lines.join("\n");
                collectorLogEl.scrollTop = collectorLogEl.scrollHeight;
            }
        }

    } catch (err) {
        console.error("refreshJobsSummary error", err);
    }
}

// ---- Collector controls ----

async function startCollector() {
    try {
        if (collectorRunBtn) collectorRunBtn.disabled = true;
        if (collectorStopBtn) collectorStopBtn.disabled = false;
        if (collectorLogEl) collectorLogEl.textContent = "";
        const res = await fetch("/collector/run", {method: "POST"});
        if (!res.ok) {
            const text = await res.text();
            throw new Error(text || ("HTTP " + res.status));
        }
        if (collectorStatusEl) {
            collectorStatusEl.innerHTML =
                '<span class="pill"><span class="dot"></span><span>در حال اجرا...</span></span>';
        }
        collectorRunning = true;
        refreshJobsSummary();
    } catch (err) {
        console.error(err);
        if (collectorRunBtn) collectorRunBtn.disabled = false;
        if (collectorStopBtn) collectorStopBtn.disabled = true;
        if (collectorStatusEl) {
            collectorStatusEl.innerHTML =
                '<span class="pill red"><span class="dot"></span><span>خطا در شروع کالکتور</span></span>';
        }
    }
}

async function stopCollector() {
    try {
        if (collectorStopBtn) collectorStopBtn.disabled = true;
        const res = await fetch("/collector/stop", {method: "POST"});
        if (!res.ok) {
            const text = await res.text();
            throw new Error(text || ("HTTP " + res.status));
        }
        if (collectorStatusEl) {
            collectorStatusEl.innerHTML =
                '<span class="pill red"><span class="dot"></span><span>در حال توقف...</span></span>';
        }
        refreshJobsSummary();
    } catch (err) {
        console.error(err);
        if (collectorStatusEl) {
            collectorStatusEl.innerHTML =
                '<span class="pill red"><span class="dot"></span><span>خطا در توقف کالکتور</span></span>';
        }
    }
}

// ---- Importer controls ----

async function startImporter() {
    try {
        if (importerRunBtn) importerRunBtn.disabled = true;
        if (importerStopBtn) importerStopBtn.disabled = false;
        if (collectorLogEl) collectorLogEl.textContent = "";
        const res = await fetch("/importer/run", {method: "POST"});
        if (!res.ok) {
            const text = await res.text();
            throw new Error(text || ("HTTP " + res.status));
        }
        if (importerStatusEl) {
            importerStatusEl.innerHTML =
                '<span class="pill"><span class="dot"></span><span>در حال اجرا...</span></span>';
        }
        importerRunning = true;
        refreshJobsSummary();
    } catch (err) {
        console.error(err);
        if (importerRunBtn) importerRunBtn.disabled = false;
        if (importerStopBtn) importerStopBtn.disabled = true;
        if (importerStatusEl) {
            importerStatusEl.innerHTML =
                '<span class="pill red"><span class="dot"></span><span>خطا در شروع ایمپورتر</span></span>';
        }
    }
}

async function stopImporter() {
    try {
        if (importerStopBtn) importerStopBtn.disabled = true;
        const res = await fetch("/importer/stop", {method: "POST"});
        if (!res.ok) {
            const text = await res.text();
            throw new Error(text || ("HTTP " + res.status));
        }
        if (importerStatusEl) {
            importerStatusEl.innerHTML =
                '<span class="pill red"><span class="dot"></span><span>در حال توقف ایمپورتر</span></span>';
        }
        refreshJobsSummary();
    } catch (err) {
        console.error(err);
        if (importerStatusEl) {
            importerStatusEl.innerHTML =
                '<span class="pill red"><span class="dot"></span><span>خطا در توقف ایمپورتر</span></span>';
        }
    }
}

// ---- JSON updater controls ----

async function startJson() {
    try {
        if (jsonRunBtn) jsonRunBtn.disabled = true;
        if (jsonStopBtn) jsonStopBtn.disabled = false;
        if (collectorLogEl) collectorLogEl.textContent = "";
        const res = await fetch("/json/run", {method: "POST"});
        if (!res.ok) {
            const text = await res.text();
            throw new Error(text || ("HTTP " + res.status));
        }
        if (jsonStatusEl) {
            jsonStatusEl.innerHTML =
                '<span class="pill"><span class="dot"></span><span>در حال اجرا...</span></span>';
        }
        jsonRunning = true;
        refreshJobsSummary();
    } catch (err) {
        console.error(err);
        if (jsonRunBtn) jsonRunBtn.disabled = false;
        if (jsonStopBtn) jsonStopBtn.disabled = true;
        if (jsonStatusEl) {
            jsonStatusEl.innerHTML =
                '<span class="pill red"><span class="dot"></span><span>خطا در شروع JSON updater</span></span>';
        }
    }
}

async function stopJson() {
    try {
        if (jsonStopBtn) jsonStopBtn.disabled = true;
        const res = await fetch("/json/stop", {method: "POST"});
        if (!res.ok) {
            const text = await res.text();
            throw new Error(text || ("HTTP " + res.status));
        }
        if (jsonStatusEl) {
            jsonStatusEl.innerHTML =
                '<span class="pill red"><span class="dot"></span><span>در حال توقف JSON updater</span></span>';
        }
        refreshJobsSummary();
    } catch (err) {
        console.error(err);
        if (jsonStatusEl) {
            jsonStatusEl.innerHTML =
                '<span class="pill red"><span class="dot"></span><span>خطا در توقف JSON updater</span></span>';
        }
    }
}

// ---- Hash updater controls ----

async function startHash() {
    try {
        if (hashRunBtn) hashRunBtn.disabled = true;
        if (hashStopBtn) hashStopBtn.disabled = false;
        if (collectorLogEl) collectorLogEl.textContent = "";
        const res = await fetch("/hash/run", {method: "POST"});
        if (!res.ok) {
            const text = await res.text();
            throw new Error(text || ("HTTP " + res.status));
        }
        if (hashStatusEl) {
            hashStatusEl.innerHTML =
                '<span class="pill"><span class="dot"></span><span>در حال اجرا...</span></span>';
        }
        hashRunning = true;
        refreshJobsSummary();
    } catch (err) {
        console.error(err);
        if (hashRunBtn) hashRunBtn.disabled = false;
        if (hashStopBtn) hashStopBtn.disabled = true;
        if (hashStatusEl) {
            hashStatusEl.innerHTML =
                '<span class="pill red"><span class="dot"></span><span>خطا در شروع Hash updater</span></span>';
        }
    }
}

async function stopHash() {
    try {
        if (hashStopBtn) hashStopBtn.disabled = true;
        const res = await fetch("/hash/stop", {method: "POST"});
        if (!res.ok) {
            const text = await res.text();
            throw new Error(text || ("HTTP " + res.status));
        }
        if (hashStatusEl) {
            hashStatusEl.innerHTML =
                '<span class="pill red"><span class="dot"></span><span>در حال توقف Hash updater</span></span>';
        }
        refreshJobsSummary();
    } catch (err) {
        console.error(err);
        if (hashStatusEl) {
            hashStatusEl.innerHTML =
                '<span class="pill red"><span class="dot"></span><span>خطا در توقف Hash updater</span></span>';
        }
    }
}

// ---- Compress (DB backup) controls ----

async function startCompress() {
    try {
        if (compressRunBtn) compressRunBtn.disabled = true;
        if (collectorLogEl) collectorLogEl.textContent = "";
        const res = await fetch("/compress/run", {method: "POST"});
        if (!res.ok) {
            const text = await res.text();
            throw new Error(text || ("HTTP " + res.status));
        }
        if (compressStatusEl) {
            compressStatusEl.innerHTML =
                '<span class="pill"><span class="dot"></span><span>در حال اجرا...</span></span>';
        }
        compressRunning = true;
        refreshJobsSummary();
    } catch (err) {
        console.error(err);
        if (compressRunBtn) compressRunBtn.disabled = false;
        if (compressStatusEl) {
            compressStatusEl.innerHTML =
                '<span class="pill red"><span class="dot"></span><span>خطا در شروع بکاپ</span></span>';
        }
    }
}

// ---- onload ----

window.addEventListener("load", () => {
    refreshHealth();
    refreshJobsSummary();
    // هر ۳ ثانیه یک snapshot از وضعیت jobها + لاگ
    setInterval(refreshJobsSummary, 3000);
});
