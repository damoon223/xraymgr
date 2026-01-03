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
  const sqlPresetApplyBtn = document.getElementById("sql-preset-apply-btn");

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

  // --- Test tab ---
  const testStatusEl = document.getElementById("test-status");
  const testStatsEl = document.getElementById("test-stats");
  const testLogEl = document.getElementById("test-log");
  const testRunBtn = document.getElementById("test-run-btn");
  const testStopBtn = document.getElementById("test-stop-btn");
  const testRefreshBtn = document.getElementById("test-refresh-btn");

  // --- Settings tab ---
  const settingsPathEl = document.getElementById("settings-path");
  const settingsStatusEl = document.getElementById("settings-status");

  const setTestCount = document.getElementById("set-test-count");
  const setTestParallel = document.getElementById("set-test-parallel");
  const setTestPortStart = document.getElementById("set-test-port-start");
  const setTestPortCount = document.getElementById("set-test-port-count");
  const setTestTagPrefix = document.getElementById("set-test-tag-prefix");
  const setTestTimeout = document.getElementById("set-test-timeout");
  const setTestSocksUser = document.getElementById("set-test-socks-user");
  const setTestSocksPass = document.getElementById("set-test-socks-pass");
  const setTestXrayBin = document.getElementById("set-test-xray-bin");
  const setTestApiServer = document.getElementById("set-test-api-server");
  const setTestSocksListen = document.getElementById("set-test-socks-listen");

  const setBaseRouting = document.getElementById("set-base-routing");

  const settingsSqlPresetsContainer = document.getElementById("settings-sql-presets");
  const sqlPresetAddBtn = document.getElementById("sql-preset-add-btn");

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

  function nowIso() {
    try {
      return new Date().toISOString().replace("T", " ").replace("Z", " UTC");
    } catch {
      return String(Date.now());
    }
  }

  // -------- Tab --------
  let activeTab = "jobs";

  function switchTab(target) {
    activeTab = target;
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

  // -------- Presets (از settings) --------
  let settingsModel = null;
  let settingsPath = "";
  let settingsLoaded = false;

  function normalizeSqlPresets(list) {
    if (!Array.isArray(list)) return [];
    const out = [];
    for (const it of list) {
      if (!it) continue;
      const label = String(it.label ?? "").trim();
      const query = String(it.query ?? "").trim();
      if (!label || !query) continue;
      out.push({ label, query });
    }
    return out;
  }

  function rebuildSqlPresetSelect() {
    if (!sqlPresets) return;
    const presets = normalizeSqlPresets(settingsModel?.sql_presets);

    sqlPresets.innerHTML = "";
    const first = document.createElement("option");
    first.value = "";
    first.textContent = "کوئری آماده…";
    sqlPresets.appendChild(first);

    presets.forEach((p, idx) => {
      const opt = document.createElement("option");
      opt.value = String(idx);
      opt.textContent = p.label;
      sqlPresets.appendChild(opt);
    });
  }

  if (sqlPresets && sqlInput) {
    sqlPresets.addEventListener("change", () => {
      const idx = Number(sqlPresets.value);
      const presets = normalizeSqlPresets(settingsModel?.sql_presets);
      if (!Number.isFinite(idx) || idx < 0 || idx >= presets.length) return;
      sqlInput.value = presets[idx].query;
    });
  }

  if (sqlPresetApplyBtn && sqlInput && sqlPresets) {
    sqlPresetApplyBtn.addEventListener("click", () => {
      const idx = Number(sqlPresets.value);
      const presets = normalizeSqlPresets(settingsModel?.sql_presets);
      if (!Number.isFinite(idx) || idx < 0 || idx >= presets.length) return;
      sqlInput.value = presets[idx].query;
      sqlInput.focus();
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

  // -------- Test Log --------
  let testOffset = 0;
  let testLines = [];
  let testInFlight = false;

  function pushTestLines(lines) {
    if (!Array.isArray(lines) || !lines.length) return;

    for (const ln of lines) {
      const t = String(ln ?? "").trimEnd();
      if (t) testLines.push(t);
    }

    // برای UI: آخرین 400 خط
    if (testLines.length > 400) testLines = testLines.slice(-400);

    if (testLogEl) {
      testLogEl.textContent = testLines.join("\n") || "(هنوز لاگی ثبت نشده است)";
      requestAnimationFrame(() => {
        testLogEl.scrollTop = testLogEl.scrollHeight;
      });
    }
  }

  async function pollTestLogOnce() {
    if (testInFlight) return;
    testInFlight = true;

    try {
      const data = await fetchJson(`/test/log?offset=${testOffset}`, {}, 8000);
      const lines = data.lines || [];
      pushTestLines(lines);

      if (typeof data.total === "number") {
        testOffset = data.total;
      } else {
        testOffset += Array.isArray(lines) ? lines.length : 0;
      }
    } catch (e) {
      // هیچ چیزی را قفل نکن
    } finally {
      testInFlight = false;
    }
  }

  function clearTestLogClientSide() {
    testOffset = 0;
    testLines = [];
    if (testLogEl) testLogEl.textContent = "";
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

    if (key === "test") {
      // اگر از summary استفاده شد
      applyTestStatus(st);
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
    setButtons(testRunBtn, testStopBtn, false);
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
      if (jobs.test) applyJobState("test", jobs.test);
    } catch (e) {
      console.error(e);
      setUnknownStatesToSafeDefaults();
    } finally {
      summaryInFlight = false;
    }
  }

  // -------- Job actions --------
  async function postJson(url, bodyObj = null, timeoutMs = 8000) {
    const opts = { method: "POST" };
    if (bodyObj !== null) {
      opts.headers = { "Content-Type": "application/json" };
      opts.body = JSON.stringify(bodyObj);
    }
    const data = await fetchJson(url, opts, timeoutMs);
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

  // -------- Test status + actions --------
  function applyTestStatus(st) {
    const running = !!st?.running;
    safeText(testStatusEl, running ? "running" : "idle");
    renderStats(testStatsEl, st?.stats || null);
    setButtons(testRunBtn, testStopBtn, running);
  }

  let testStatusInFlight = false;

  async function refreshTestStatusOnce() {
    if (testStatusInFlight) return;
    testStatusInFlight = true;

    try {
      const st = await fetchJson("/test/status", {}, 6000);
      applyTestStatus(st);
    } catch (e) {
      // اگر endpoint نبود/خطا داشت، حداقل UI قفل نشود
    } finally {
      testStatusInFlight = false;
    }
  }

  async function runTest() {
    // تست از settings خوانده می‌شود، پس payload خالی
    try {
      clearTestLogClientSide();
      safeText(testStatusEl, "starting...");
      if (testLogEl) {
        testLogEl.textContent = `[${nowIso()}] start requested...`;
      }
      await postJson("/test/run", {});
    } catch (e) {
      console.error(e);
      const msg = e && e.name === "AbortError" ? "Timeout" : (e?.message || e);
      if (testLogEl) testLogEl.textContent = `[${nowIso()}] ERROR: ${msg}`;
    } finally {
      await refreshTestStatusOnce();
      pollTestLogOnce();
    }
  }

  async function stopTest() {
    try {
      await postJson("/test/stop", {});
    } catch (e) {
      console.error(e);
    } finally {
      await refreshTestStatusOnce();
      pollTestLogOnce();
    }
  }

  if (testRunBtn) testRunBtn.addEventListener("click", runTest);
  if (testStopBtn) testStopBtn.addEventListener("click", stopTest);
  if (testRefreshBtn) testRefreshBtn.addEventListener("click", async () => {
    await refreshTestStatusOnce();
    pollTestLogOnce();
  });

  // -------- Settings UI --------
  function setStatusLine(text) {
    if (!settingsStatusEl) return;
    settingsStatusEl.textContent = text || "";
  }

  function getIntVal(el, defVal) {
    if (!el) return defVal;
    const v = Number(el.value);
    return Number.isFinite(v) ? v : defVal;
  }

  function getStrVal(el, defVal) {
    if (!el) return defVal;
    const v = String(el.value ?? "");
    return v;
  }

  function ensureSettingsShape() {
    if (!settingsModel || typeof settingsModel !== "object") settingsModel = {};
    if (!settingsModel.test || typeof settingsModel.test !== "object") settingsModel.test = {};
    if (!Array.isArray(settingsModel.sql_presets)) settingsModel.sql_presets = [];
    if (settingsModel.base_routing === undefined) settingsModel.base_routing = {};
  }

  function renderSettingsForm() {
    if (!settingsLoaded) return;
    ensureSettingsShape();

    safeText(settingsPathEl, settingsPath || "...");

    // test
    if (setTestCount) setTestCount.value = String(settingsModel.test.count ?? 100);
    if (setTestParallel) setTestParallel.value = String(settingsModel.test.parallel ?? 10);
    if (setTestPortStart) setTestPortStart.value = String(settingsModel.test.port_start ?? 9000);
    if (setTestPortCount) setTestPortCount.value = String(settingsModel.test.port_count ?? 100);
    if (setTestTagPrefix) setTestTagPrefix.value = String(settingsModel.test.inbound_tag_prefix ?? "in_test_");
    if (setTestTimeout) setTestTimeout.value = String(settingsModel.test.check_timeout_sec ?? 60);
    if (setTestSocksUser) setTestSocksUser.value = String(settingsModel.test.socks_user ?? "me");
    if (setTestSocksPass) setTestSocksPass.value = String(settingsModel.test.socks_pass ?? "1");
    if (setTestXrayBin) setTestXrayBin.value = String(settingsModel.test.xray_bin ?? "xray");
    if (setTestApiServer) setTestApiServer.value = String(settingsModel.test.api_server ?? "127.0.0.1:10085");
    if (setTestSocksListen) setTestSocksListen.value = String(settingsModel.test.socks_listen ?? "127.0.0.1");

    // base_routing: textarea برای JSON
    if (setBaseRouting) {
      try {
        setBaseRouting.value = JSON.stringify(settingsModel.base_routing ?? {}, null, 2);
      } catch {
        setBaseRouting.value = "{}";
      }
    }

    renderSqlPresetsEditor();
    rebuildSqlPresetSelect();
  }

  function renderSqlPresetsEditor() {
    if (!settingsSqlPresetsContainer) return;
    ensureSettingsShape();

    const presets = normalizeSqlPresets(settingsModel.sql_presets);
    // نگه داشتن ترتیب اصلی اما تمیزسازی سطرهای خالی
    settingsModel.sql_presets = presets;

    settingsSqlPresetsContainer.innerHTML = "";

    if (!presets.length) {
      const empty = document.createElement("div");
      empty.className = "muted";
      empty.textContent = "هیچ presetی ثبت نشده است.";
      settingsSqlPresetsContainer.appendChild(empty);
      return;
    }

    presets.forEach((p, idx) => {
      const wrap = document.createElement("div");

      const labelRow = document.createElement("div");
      labelRow.className = "label-row";
      const l1 = document.createElement("div");
      l1.textContent = `preset #${idx + 1} label`;
      const l2 = document.createElement("div");
      l2.className = "muted";
      l2.textContent = "نام نمایشی";
      labelRow.appendChild(l1);
      labelRow.appendChild(l2);

      const labelInput = document.createElement("input");
      labelInput.type = "text";
      labelInput.value = p.label;

      const queryRow = document.createElement("div");
      queryRow.className = "label-row";
      const q1 = document.createElement("div");
      q1.textContent = "query";
      const q2 = document.createElement("div");
      q2.className = "muted";
      q2.textContent = "SQL";
      queryRow.appendChild(q1);
      queryRow.appendChild(q2);

      const queryInput = document.createElement("input");
      queryInput.type = "text";
      queryInput.value = p.query;

      const btnRow = document.createElement("div");
      btnRow.className = "btn-row";
      const delBtn = document.createElement("button");
      delBtn.type = "button";
      delBtn.className = "danger";
      delBtn.textContent = "حذف";
      btnRow.appendChild(delBtn);

      labelInput.addEventListener("input", () => {
        settingsModel.sql_presets[idx].label = labelInput.value;
        scheduleSaveSettings();
        rebuildSqlPresetSelect();
      });

      queryInput.addEventListener("input", () => {
        settingsModel.sql_presets[idx].query = queryInput.value;
        scheduleSaveSettings();
        rebuildSqlPresetSelect();
      });

      delBtn.addEventListener("click", () => {
        settingsModel.sql_presets.splice(idx, 1);
        scheduleSaveSettings();
        renderSqlPresetsEditor();
        rebuildSqlPresetSelect();
      });

      wrap.appendChild(labelRow);
      wrap.appendChild(labelInput);
      wrap.appendChild(queryRow);
      wrap.appendChild(queryInput);
      wrap.appendChild(btnRow);

      settingsSqlPresetsContainer.appendChild(wrap);
    });
  }

  // --- auto-save (debounced) ---
  let saveTimer = null;
  let saveInFlight = false;
  let saveQueued = false;

  function scheduleSaveSettings() {
    if (!settingsLoaded) return;
    setStatusLine("در حال ذخیره...");

    if (saveTimer) clearTimeout(saveTimer);
    saveTimer = setTimeout(() => {
      saveTimer = null;
      void saveSettings();
    }, 400);
  }

  async function saveSettings() {
    if (!settingsLoaded) return;
    if (saveInFlight) {
      saveQueued = true;
      return;
    }

    ensureSettingsShape();

    // base_routing parse
    if (setBaseRouting) {
      const txt = String(setBaseRouting.value || "").trim();
      if (txt) {
        try {
          settingsModel.base_routing = JSON.parse(txt);
        } catch {
          setStatusLine("base_routing JSON نامعتبر است.");
          return;
        }
      } else {
        settingsModel.base_routing = {};
      }
    }

    // test settings read
    settingsModel.test.count = getIntVal(setTestCount, 100);
    settingsModel.test.parallel = getIntVal(setTestParallel, 10);
    settingsModel.test.port_start = getIntVal(setTestPortStart, 9000);
    settingsModel.test.port_count = getIntVal(setTestPortCount, 100);
    settingsModel.test.inbound_tag_prefix = getStrVal(setTestTagPrefix, "in_test_");
    settingsModel.test.check_timeout_sec = getIntVal(setTestTimeout, 60);
    settingsModel.test.socks_user = getStrVal(setTestSocksUser, "me");
    settingsModel.test.socks_pass = getStrVal(setTestSocksPass, "1");
    settingsModel.test.xray_bin = getStrVal(setTestXrayBin, "xray");
    settingsModel.test.api_server = getStrVal(setTestApiServer, "127.0.0.1:10085");
    settingsModel.test.socks_listen = getStrVal(setTestSocksListen, "127.0.0.1");

    // cleanup sql presets
    settingsModel.sql_presets = normalizeSqlPresets(settingsModel.sql_presets);

    saveInFlight = true;
    saveQueued = false;

    try {
      const resp = await fetchJson(
        "/settings",
        {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ settings: settingsModel }),
        },
        12000
      );

      settingsPath = resp.path || settingsPath || "";
      settingsModel = resp.settings || settingsModel;
      setStatusLine("ذخیره شد.");
      rebuildSqlPresetSelect();
    } catch (e) {
      console.error(e);
      const msg = e && e.name === "AbortError" ? "Timeout" : (e?.message || e);
      setStatusLine("خطا در ذخیره: " + msg);
    } finally {
      saveInFlight = false;
      if (saveQueued) {
        saveQueued = false;
        scheduleSaveSettings();
      }
    }
  }

  function bindSettingsChangeHandlers() {
    const bind = (el) => {
      if (!el) return;
      el.addEventListener("input", scheduleSaveSettings);
    };

    // test fields
    bind(setTestCount);
    bind(setTestParallel);
    bind(setTestPortStart);
    bind(setTestPortCount);
    bind(setTestTagPrefix);
    bind(setTestTimeout);
    bind(setTestSocksUser);
    bind(setTestSocksPass);
    bind(setTestXrayBin);
    bind(setTestApiServer);
    bind(setTestSocksListen);

    if (setBaseRouting) setBaseRouting.addEventListener("input", scheduleSaveSettings);

    if (sqlPresetAddBtn) {
      sqlPresetAddBtn.addEventListener("click", () => {
        ensureSettingsShape();
        settingsModel.sql_presets.push({ label: "new preset", query: "select 1" });
        scheduleSaveSettings();
        renderSqlPresetsEditor();
        rebuildSqlPresetSelect();
      });
    }
  }

  async function loadSettingsOnce() {
    try {
      const resp = await fetchJson("/settings", {}, 8000);
      settingsPath = resp.path || "";
      settingsModel = resp.settings || {};
      settingsLoaded = true;
      renderSettingsForm();
      bindSettingsChangeHandlers();
      setStatusLine("");
    } catch (e) {
      console.error(e);
      setStatusLine("خطا در خواندن تنظیمات.");
    }
  }

  // -------- Loop --------
  // Summary هر 2 ثانیه
  // Log هر 1 ثانیه (و هر درخواست timeout دارد تا “استاپ شدن” اتفاق نیفتد)
  function startLoops() {
    refreshSummaryOnce();
    pollLogOnce();
    refreshTestStatusOnce();
    pollTestLogOnce();

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

    // تست: اگر تب تست باز است یا خود تست running است، لاگ را سریع‌تر بخوان
    setInterval(() => {
      refreshTestStatusOnce();

      if (activeTab === "test") {
        pollTestLogOnce();
      } else {
        if (Date.now() % 3000 < 1000) pollTestLogOnce();
      }
    }, 1000);
  }

  // init
  switchTab("jobs");
  setUnknownStatesToSafeDefaults();
  startLoops();
  loadSettingsOnce();
})();
