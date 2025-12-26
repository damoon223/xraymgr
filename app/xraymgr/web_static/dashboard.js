(() => {
  "use strict";

  // -------- عناصر --------
  const dbPathEl = document.getElementById("db-path");
  const footerDbPathEl = document.getElementById("footer-db-path");

  const sqlInput = document.getElementById("sql-input");
  const sqlStatus = document.getElementById("sql-status");
  const sqlResult = document.getElementById("sql-result");
  const sqlPresets = document.getElementById("sql-presets");
  const sqlRunBtn = document.getElementById("sql-run-btn");

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

  const jsonRepairStatusEl = document.getElementById("json-repair-status");
  const jsonRepairStatsEl = document.getElementById("json-repair-stats");
  const jsonRepairRunBtn = document.getElementById("json-repair-run-btn");
  const jsonRepairStopBtn = document.getElementById("json-repair-stop-btn");

  const hashStatusEl = document.getElementById("hash-status");
  const hashStatsEl = document.getElementById("hash-stats");
  const hashRunBtn = document.getElementById("hash-run-btn");
  const hashStopBtn = document.getElementById("hash-stop-btn");

  const groupStatusEl = document.getElementById("group-status");
  const groupStatsEl = document.getElementById("group-stats");
  const groupRunBtn = document.getElementById("group-run-btn");
  const groupStopBtn = document.getElementById("group-stop-btn");

  const compressStatusEl = document.getElementById("compress-status");
  const compressStatsEl = document.getElementById("compress-stats");
  const compressRunBtn = document.getElementById("compress-run-btn");

  const tabButtons = document.querySelectorAll(".tab-button");
  const tabContents = document.querySelectorAll(".tab-content");

  // -------- ابزارها --------
  function escapeHtml(str) {
    return String(str)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;");
  }

  function safeText(el, text) {
    if (!el) return;
    el.textContent = text;
  }

  function safeHtml(el, html) {
    if (!el) return;
    el.innerHTML = html;
  }

  function setButtons(runBtn, stopBtn, running) {
    if (runBtn) runBtn.disabled = !!running;
    if (stopBtn) stopBtn.disabled = !running;
  }

  function withTimeout(ms) {
    // timeout برای fetch تا هیچ ریکوئستی پنل را قفل نکند
    const controller = new AbortController();
    const id = setTimeout(() => controller.abort(), ms);
    return { signal: controller.signal, cancel: () => clearTimeout(id) };
  }

  async function fetchJson(url, opts = {}, timeoutMs = 8000) {
    const t = withTimeout(timeoutMs);
    try {
      const res = await fetch(url, {
        cache: "no-store",
        ...opts,
        signal: t.signal,
      });
      if (!res.ok) {
        const txt = await res.text().catch(() => "");
        throw new Error(txt || ("HTTP " + res.status));
      }
      return await res.json().catch(() => ({}));
    } finally {
      t.cancel();
    }
  }

  // -------- Tab --------
  function switchTab(target) {
    tabButtons.forEach((btn) => {
      btn.classList.toggle("active", btn.dataset.tab === target);
    });
    tabContents.forEach((tab) => {
      tab.classList.toggle("active", tab.id === "tab-" + target);
    });
  }

  tabButtons.forEach((btn) => {
    btn.addEventListener("click", () => switchTab(btn.dataset.tab));
  });

  // -------- Presets --------
  const PRESET_QUERIES = [
    { label: "کوئری آماده…", value: "" },
    { label: "۱۰ سطر اول links", value: "select * from links limit 10" },
    { label: "select count(*) from links", value: "select count(*) from links" },
    {
      label: "invalid=1 و unsupported=0 (50)",
      value:
        "select url,config_json,is_invalid,is_protocol_unsupported from links where is_invalid=1 and is_protocol_unsupported=0 limit 50",
    },
    { label: "select * from links where url='%%'", value: "select * from links where url='%%'" },
    { label: "delete from links", value: "delete from links" },
  ];

  function ensurePresets() {
    if (!sqlPresets) return;
    sqlPresets.innerHTML = "";
    PRESET_QUERIES.forEach((p) => {
      const opt = document.createElement("option");
      opt.value = p.value;
      opt.textContent = p.label;
      sqlPresets.appendChild(opt);
    });
  }

  if (sqlPresets && sqlInput) {
    ensurePresets();
    sqlPresets.addEventListener("change", () => {
      const v = sqlPresets.value || "";
      if (v) sqlInput.value = v;
    });
  }

  // -------- SQL Query --------
  function renderSqlTable(data) {
    if (!sqlResult) return;

    const cols = Array.isArray(data.columns) ? data.columns : [];
    const rows = Array.isArray(data.rows) ? data.rows : [];

    if (!cols.length) {
      safeHtml(
        sqlResult,
        `<div class="muted">کوئری بدون نتیجهٔ جدولی اجرا شد. (rowcount=${escapeHtml(
          data.rowcount ?? ""
        )})</div>`
      );
      return;
    }

    let html = `<table class="db-table"><thead><tr>`;
    for (const c of cols) html += `<th>${escapeHtml(c)}</th>`;
    html += `</tr></thead><tbody>`;

    if (!rows.length) {
      html += `<tr><td colspan="${cols.length}" class="muted">داده‌ای وجود ندارد.</td></tr>`;
    } else {
      for (const row of rows) {
        html += `<tr>`;
        for (const c of cols) {
          let v = row[c];
          if (v === null || v === undefined) v = "";
          html += `<td>${escapeHtml(String(v))}</td>`;
        }
        html += `</tr>`;
      }
    }

    html += `</tbody></table>`;
    safeHtml(sqlResult, html);
  }

  let sqlInFlight = false;

  async function runQuery() {
    if (!sqlInput || !sqlStatus || !sqlResult) return;
    if (sqlInFlight) return;
    sqlInFlight = true;

    const q = sqlInput.value || "";
    safeText(sqlStatus, "");
    safeHtml(sqlResult, `<div class="muted">...</div>`);

    if (!q.trim()) {
      safeText(sqlStatus, "کوئری خالی است.");
      safeHtml(sqlResult, `<div class="muted">نتیجه‌ای برای نمایش نیست.</div>`);
      sqlInFlight = false;
      return;
    }

    try {
      const data = await fetchJson(
        "/db/query",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ query: q, params: [] }),
        },
        15000
      );

      renderSqlTable(data);

      if (Array.isArray(data.columns) && data.columns.length) {
        safeText(sqlStatus, `نتیجه: ${Array.isArray(data.rows) ? data.rows.length : 0} ردیف`);
      } else {
        safeText(sqlStatus, `rowcount: ${data.rowcount ?? 0}`);
      }
    } catch (e) {
      console.error(e);
      const msg = e && e.name === "AbortError" ? "Timeout" : (e?.message || e);
      safeText(sqlStatus, "خطا در اجرای کوئری: " + msg);
      safeHtml(sqlResult, `<div class="muted">خطا در اجرای کوئری.</div>`);
    } finally {
      sqlInFlight = false;
    }
  }

  if (sqlRunBtn) sqlRunBtn.addEventListener("click", runQuery);
  if (sqlInput) {
    sqlInput.addEventListener("keydown", (ev) => {
      if (ev.key === "Enter" && (ev.ctrlKey || ev.metaKey)) runQuery();
    });
  }

  // -------- Shared Log (فقط یک منبع) --------
  let logOffset = 0;
  let lastLogTrimAt = Date.now();
  let logLines = [];
  let logInFlight = false;

  function pushLogLines(lines) {
    if (!Array.isArray(lines) || !lines.length) return;

    for (const ln of lines) {
      const t = String(ln ?? "").trimEnd();
      if (t) logLines.push(t);
    }

    if (logLines.length > 20) logLines = logLines.slice(-20);

    if (Date.now() - lastLogTrimAt >= 10000) {
      if (logLines.length > 20) logLines = logLines.slice(-20);
      lastLogTrimAt = Date.now();
    }

    if (collectorLogEl) {
      collectorLogEl.textContent = logLines.join("\n") || "(هنوز لاگی ثبت نشده است)";
      // اتو اسکرول مطمئن‌تر (بعد از رندر)
      requestAnimationFrame(() => {
        collectorLogEl.scrollTop = collectorLogEl.scrollHeight;
      });
    }
  }

  async function pollLogOnce() {
    if (logInFlight) return;
    logInFlight = true;

    try {
      const data = await fetchJson(`/collector/log?offset=${logOffset}`, {}, 6000);

      const lines = data.lines || [];
      pushLogLines(lines);

      // اگر سرور آفست را ریست کرده باشد، ما هم آفست را با total همگام می‌کنیم
      if (typeof data.total === "number") {
        logOffset = data.total;
      } else {
        logOffset += Array.isArray(lines) ? lines.length : 0;
      }
    } catch (e) {
      // هیچ چیزی را قفل نکن؛ فقط رها کن تا راند بعدی ادامه بدهد
      // (AbortError هم اینجا می‌آید)
    } finally {
      logInFlight = false;
    }
  }

  function clearLogClientSide() {
    logOffset = 0;
    logLines = [];
    if (collectorLogEl) collectorLogEl.textContent = "";
  }

  // -------- Jobs Summary (یک درخواست برای همه statusها) --------
  function renderStats(el, statsObj) {
    if (!el) return;
    if (!statsObj) {
      el.textContent = "";
      return;
    }
    try {
      el.textContent = JSON.stringify(statsObj, null, 2);
    } catch {
      el.textContent = String(statsObj);
    }
  }

  function applyJobState(key, st) {
    const running = !!st?.running;

    if (key === "collector") {
      safeText(collectorStatusEl, running ? "running" : "idle");
      renderStats(collectorStatsEl, st?.stats || null);
      setButtons(collectorRunBtn, collectorStopBtn, running);
      return;
    }

    if (key === "importer") {
      safeText(importerStatusEl, running ? "running" : "idle");
      renderStats(importerStatsEl, st?.stats || null);
      setButtons(importerRunBtn, importerStopBtn, running);
      return;
    }

    if (key === "json") {
      safeText(jsonStatusEl, running ? "running" : "idle");
      renderStats(jsonStatsEl, st?.stats || null);
      setButtons(jsonRunBtn, jsonStopBtn, running);
      return;
    }

    if (key === "json_repair") {
      if (!jsonRepairStatusEl) return;
      safeText(jsonRepairStatusEl, running ? "running" : "idle");
      renderStats(jsonRepairStatsEl, st?.stats || null);
      setButtons(jsonRepairRunBtn, jsonRepairStopBtn, running);
      return;
    }

    if (key === "hash") {
      safeText(hashStatusEl, running ? "running" : "idle");
      renderStats(hashStatsEl, st?.stats || null);
      setButtons(hashRunBtn, hashStopBtn, running);
      return;
    }

    if (key === "group") {
      safeText(groupStatusEl, running ? "running" : "idle");
      renderStats(groupStatsEl, st?.stats || null);
      setButtons(groupRunBtn, groupStopBtn, running);
      return;
    }

    if (key === "compress") {
      safeText(compressStatusEl, running ? "running" : "idle");
      renderStats(compressStatsEl, st?.stats || null);
      if (compressRunBtn) compressRunBtn.disabled = running;
      return;
    }
  }

  function setUnknownStatesToSafeDefaults() {
    setButtons(collectorRunBtn, collectorStopBtn, false);
    setButtons(importerRunBtn, importerStopBtn, false);
    setButtons(jsonRunBtn, jsonStopBtn, false);
    if (jsonRepairStatusEl) setButtons(jsonRepairRunBtn, jsonRepairStopBtn, false);
    setButtons(hashRunBtn, hashStopBtn, false);
    setButtons(groupRunBtn, groupStopBtn, false);
    if (compressRunBtn) compressRunBtn.disabled = false;
  }

  let summaryInFlight = false;

  async function refreshSummaryOnce() {
    if (summaryInFlight) return;
    summaryInFlight = true;

    try {
      const data = await fetchJson("/jobs/summary", {}, 8000);

      const dbp = data.db_path || "unknown";
      if (dbPathEl) dbPathEl.textContent = dbp;
      if (footerDbPathEl) footerDbPathEl.textContent = dbp;

      const jobs = data.jobs || {};
      applyJobState("collector", jobs.collector);
      applyJobState("importer", jobs.importer);
      applyJobState("json", jobs.json);
      applyJobState("json_repair", jobs.json_repair);
      applyJobState("hash", jobs.hash);
      applyJobState("group", jobs.group);
      applyJobState("compress", jobs.compress);
    } catch (e) {
      console.error(e);
      setUnknownStatesToSafeDefaults();
    } finally {
      summaryInFlight = false;
    }
  }

  // -------- Job actions --------
  async function postJson(url) {
    const data = await fetchJson(url, { method: "POST" }, 8000);
    return data;
  }

  function bindJobButtons(runBtn, stopBtn, runUrl, stopUrl) {
    if (runBtn) {
      runBtn.addEventListener("click", async () => {
        try {
          await postJson(runUrl);
        } catch (e) {
          console.error(e);
        } finally {
          clearLogClientSide();
          await refreshSummaryOnce();
          // لاگ را سریع شروع کن
          pollLogOnce();
        }
      });
    }

    if (stopBtn) {
      stopBtn.addEventListener("click", async () => {
        try {
          await postJson(stopUrl);
        } catch (e) {
          console.error(e);
        } finally {
          await refreshSummaryOnce();
          pollLogOnce();
        }
      });
    }
  }

  bindJobButtons(collectorRunBtn, collectorStopBtn, "/collector/run", "/collector/stop");
  bindJobButtons(importerRunBtn, importerStopBtn, "/importer/run", "/importer/stop");
  bindJobButtons(jsonRunBtn, jsonStopBtn, "/json/run", "/json/stop");
  if (jsonRepairStatusEl) {
    bindJobButtons(jsonRepairRunBtn, jsonRepairStopBtn, "/json_repair/run", "/json_repair/stop");
  }
  bindJobButtons(hashRunBtn, hashStopBtn, "/hash/run", "/hash/stop");
  bindJobButtons(groupRunBtn, groupStopBtn, "/group/run", "/group/stop");

  if (compressRunBtn) {
    compressRunBtn.addEventListener("click", async () => {
      try {
        await postJson("/compress/run");
      } catch (e) {
        console.error(e);
      } finally {
        clearLogClientSide();
        await refreshSummaryOnce();
        pollLogOnce();
      }
    });
  }

  // -------- Loop --------
  // Summary هر 2 ثانیه
  // Log هر 1 ثانیه (و هر درخواست timeout دارد تا “استاپ شدن” اتفاق نیفتد)
  function startLoops() {
    refreshSummaryOnce();
    pollLogOnce();

    setInterval(() => {
      refreshSummaryOnce();
    }, 2000);

    setInterval(() => {
      // trim 10 ثانیه‌ای حتی اگر لاگ جدید نیاید هم enforce شود
      if (Date.now() - lastLogTrimAt >= 10000) {
        if (logLines.length > 20) logLines = logLines.slice(-20);
        lastLogTrimAt = Date.now();
        if (collectorLogEl) {
          collectorLogEl.textContent = logLines.join("\n") || "(هنوز لاگی ثبت نشده است)";
          requestAnimationFrame(() => {
            collectorLogEl.scrollTop = collectorLogEl.scrollHeight;
          });
        }
      }
      pollLogOnce();
    }, 1000);
  }

  // init
  switchTab("jobs");
  setUnknownStatesToSafeDefaults();
  startLoops();
})();
