/**
 * 流程状态仅存于此并通过 chrome.storage.local 持久化，页面刷新不丢进度。
 * Content Script 只负责执行单步 DOM 与上报结果。
 */

const STORAGE_FLOW = "qantas_flow_state_v1";
const STORAGE_SCRAPE = "qantas_scrape_state_v1";
const STORAGE_BG_LOG = "qantas_bg_log_v1";
const STORAGE_PANEL_POS = "floating_console_position";
const STORAGE_PANEL_COLLAPSED = "floating_console_collapsed";

const FLEX_PRICER_URL =
  "https://book.qantas.com/qf-booking/dyn/air/booking/flexPricerAvailabilityActionFromLoad";

const netWatchByTab = new Map(); // tabId -> { targetUrl, captured, startedAt, pendingRequestId, pendingUrl }
const tabKey = (tabId) => String(tabId);

async function bgLog(message, tabId = null, extra = null) {
  const t = new Date().toISOString();
  const line =
    extra === null || typeof extra === "undefined"
      ? `[${t}] ${message}`
      : `[${t}] ${message} ${JSON.stringify(extra)}`;

  try {
    const data = await chrome.storage.local.get(STORAGE_BG_LOG);
    const cur = Array.isArray(data[STORAGE_BG_LOG]) ? data[STORAGE_BG_LOG] : [];
    const next = cur.concat([line]).slice(-500);
    await chrome.storage.local.set({ [STORAGE_BG_LOG]: next });
  } catch (_) {}

  if (typeof tabId === "number") {
    try {
      await chrome.tabs.sendMessage(tabId, { type: "BG_LOG_APPEND", payload: { line } });
    } catch (_) {}
  }
}

function safeJsonStringify(obj) {
  try {
    return JSON.stringify(obj, null, 2);
  } catch (e) {
    return JSON.stringify({ error: "json_stringify_failed", message: String(e) });
  }
}

async function ensureDebuggerAttached(tabId) {
  const debuggee = { tabId };
  try {
    await chrome.debugger.attach(debuggee, "1.3");
  } catch (_) {
    // already attached / attach failed
  }
  try {
    await chrome.debugger.sendCommand(debuggee, "Network.enable", {});
  } catch (_) {}
}

async function detachDebugger(tabId) {
  try {
    await chrome.debugger.detach({ tabId });
  } catch (_) {}
}

async function sendJsonToTabForDownload(tabId, jsonText, filename) {
  await chrome.tabs.sendMessage(tabId, {
    type: "DOWNLOAD_JSON",
    payload: { jsonText, filename },
  });
}

chrome.debugger.onEvent.addListener(async (debuggee, method, params) => {
  const tabId = debuggee && typeof debuggee.tabId === "number" ? debuggee.tabId : null;
  if (tabId == null) return;
  const w = netWatchByTab.get(tabKey(tabId));
  if (!w || w.captured) return;

  try {
    if (method === "Network.responseReceived") {
      const url = params && params.response ? params.response.url : "";
      if (!url) return;
      if (!url.includes(w.targetUrl)) return;
      const requestId = params.requestId;
      if (!requestId) return;

      // 先记录命中请求，等 loadingFinished 后再获取 body，避免 -32000 No data found。
      w.pendingRequestId = requestId;
      w.pendingUrl = url;
      netWatchByTab.set(tabKey(tabId), w);
      await bgLog("命中目标接口响应", tabId, { url, requestId });
      return;
    }

    if (method === "Network.loadingFinished") {
      const requestId = params ? params.requestId : "";
      if (!requestId || requestId !== w.pendingRequestId) return;

      const url = w.pendingUrl || w.targetUrl;
      const bodyRes = await chrome.debugger.sendCommand({ tabId }, "Network.getResponseBody", {
        requestId,
      });
      const bodyText = bodyRes
        ? bodyRes.base64Encoded
          ? atob(bodyRes.body || "")
          : bodyRes.body || ""
        : "";

      let jsonText = "";
      try {
        const obj = JSON.parse(bodyText);
        jsonText = safeJsonStringify(obj);
      } catch (_) {
        jsonText = safeJsonStringify({
          url,
          capturedAt: new Date().toISOString(),
          note: "response body is not valid JSON; saved as text",
          body: bodyText,
        });
      }

      const ts = new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-");
      const filename = `qantas-flexPricerAvailabilityActionFromLoad-${ts}.json`;
      await sendJsonToTabForDownload(tabId, jsonText, filename);
      await bgLog("已发送 JSON 到页面下载", tabId, { filename, requestId });

      try {
        await chrome.tabs.sendMessage(tabId, { type: "SIMULATE_BROWSE_SCROLL" });
      } catch (_) {}

      w.captured = true;
      netWatchByTab.set(tabKey(tabId), w);
      await detachDebugger(tabId);
      return;
    }

    if (method === "Network.loadingFailed") {
      const requestId = params ? params.requestId : "";
      if (!requestId || requestId !== w.pendingRequestId) return;
      await bgLog("目标请求加载失败", tabId, {
        requestId,
        errorText: params && params.errorText ? params.errorText : "",
      });
      w.captured = true;
      netWatchByTab.set(tabKey(tabId), w);
      await detachDebugger(tabId);
      return;
    }
  } catch (e) {
    await bgLog("抓取接口响应失败", tabId, { error: String(e) });
    w.captured = true;
    netWatchByTab.set(tabKey(tabId), w);
    await detachDebugger(tabId);
  }
});

chrome.debugger.onDetach.addListener((debuggee) => {
  const tabId = debuggee && typeof debuggee.tabId === "number" ? debuggee.tabId : null;
  if (tabId == null) return;
  netWatchByTab.delete(tabKey(tabId));
});

const STEP_ORDER = [
  "dismiss",
  "trip",
  "departure",
  "arrival",
  "date_open",
  "date_pick",
  "confirm",
  "search",
  "bono_continue",
];

function defaultFlowState() {
  return {
    version: 1,
    flowStep: "idle",
    departure: "",
    arrival: "",
    travelDate: "",
    lastError: null,
    updatedAt: Date.now(),
  };
}

function defaultScrapeState() {
  return {
    version: 1,
    mode: "grouped", // grouped | upsell | unknown
    pageKey: "",
    nextItineraryIndex: 0,
    rows: [],
    updatedAt: Date.now(),
  };
}

async function loadFlowState() {
  const data = await chrome.storage.local.get(STORAGE_FLOW);
  return data[STORAGE_FLOW] || defaultFlowState();
}

async function saveFlowState(partial) {
  const cur = await loadFlowState();
  const next = { ...cur, ...partial, updatedAt: Date.now() };
  await chrome.storage.local.set({ [STORAGE_FLOW]: next });
  return next;
}

async function loadScrapeState() {
  const data = await chrome.storage.local.get(STORAGE_SCRAPE);
  return data[STORAGE_SCRAPE] || defaultScrapeState();
}

async function saveScrapeState(partial) {
  const cur = await loadScrapeState();
  const next = { ...cur, ...partial, updatedAt: Date.now() };
  await chrome.storage.local.set({ [STORAGE_SCRAPE]: next });
  return next;
}

async function resetScrapeState() {
  await chrome.storage.local.set({ [STORAGE_SCRAPE]: defaultScrapeState() });
  return loadScrapeState();
}

function nextStepAfter(current) {
  const i = STEP_ORDER.indexOf(current);
  if (i < 0 || i >= STEP_ORDER.length - 1) return "done";
  return STEP_ORDER[i + 1];
}

function stepOrderIndex(step) {
  const i = STEP_ORDER.indexOf(step);
  return i < 0 ? -1 : i;
}

chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  (async () => {
    try {
      if (msg.type === "GET_FLOW_STATE") {
        const state = await loadFlowState();
        sendResponse({ ok: true, state });
        return;
      }

      if (msg.type === "START_QANTAS_FLOW") {
        const { departure, arrival, travelDate } = msg.payload || {};
        if (!departure || !arrival || !travelDate) {
          sendResponse({ ok: false, error: "请填写出发、到达与日期" });
          return;
        }
        const iso = /^\d{4}-\d{2}-\d{2}$/.test(travelDate);
        if (!iso) {
          sendResponse({ ok: false, error: "日期格式应为 YYYY-MM-DD" });
          return;
        }
        await resetScrapeState();
        await saveFlowState({
          flowStep: "dismiss",
          departure: String(departure).toUpperCase().trim(),
          arrival: String(arrival).toUpperCase().trim(),
          travelDate: travelDate.trim(),
          lastError: null,
        });
        sendResponse({ ok: true, state: await loadFlowState() });
        return;
      }

      if (msg.type === "STEP_OK") {
        const { finishedStep } = msg.payload || {};
        const cur = await loadFlowState();
        if (cur.flowStep !== finishedStep) {
          sendResponse({ ok: true, state: cur, note: "step_mismatch_ignored" });
          return;
        }
        const nxt = nextStepAfter(finishedStep);
        await saveFlowState({
          flowStep: nxt === "done" ? "idle" : nxt,
          lastError: null,
        });
        sendResponse({ ok: true, state: await loadFlowState() });
        return;
      }

      /**
       * 仅允许向前对齐（根据新页 DOM 修正整页跳转时未发出的 STEP_OK）。
       * targetStep 必须晚于当前 flowStep。
       */
      if (msg.type === "RECONCILE_FLOW") {
        const { targetStep } = msg.payload || {};
        const cur = await loadFlowState();
        if (!targetStep || cur.flowStep === "idle" || cur.flowStep === "done") {
          sendResponse({ ok: true, state: cur, reconciled: false });
          return;
        }
        const curIdx = stepOrderIndex(cur.flowStep);
        const tgtIdx = stepOrderIndex(targetStep);
        if (tgtIdx < 0 || curIdx < 0 || tgtIdx <= curIdx) {
          sendResponse({ ok: true, state: cur, reconciled: false });
          return;
        }
        await saveFlowState({ flowStep: targetStep, lastError: null });
        sendResponse({ ok: true, state: await loadFlowState(), reconciled: true });
        return;
      }

      if (msg.type === "STEP_FAIL") {
        const { error, atStep } = msg.payload || {};
        await saveFlowState({
          flowStep: "idle",
          lastError: error || atStep || "unknown",
        });
        sendResponse({ ok: false, state: await loadFlowState() });
        return;
      }

      if (msg.type === "START_SCRAPE_ONLY") {
        await saveFlowState({
          flowStep: "scrape_results",
          lastError: null,
        });
        sendResponse({ ok: true, state: await loadFlowState() });
        return;
      }

      if (msg.type === "SCRAPE_GET_STATE") {
        sendResponse({ ok: true, state: await loadScrapeState() });
        return;
      }

      if (msg.type === "SCRAPE_RESET") {
        sendResponse({ ok: true, state: await resetScrapeState() });
        return;
      }

      if (msg.type === "SCRAPE_SET_PROGRESS") {
        const { nextItineraryIndex, mode, pageKey } = msg.payload || {};
        const s = await saveScrapeState({
          ...(typeof nextItineraryIndex === "number" ? { nextItineraryIndex } : {}),
          ...(mode ? { mode } : {}),
          ...(typeof pageKey === "string" ? { pageKey } : {}),
        });
        sendResponse({ ok: true, state: s });
        return;
      }

      if (msg.type === "SCRAPE_APPEND_ROWS") {
        const { rows } = msg.payload || {};
        const cur = await loadScrapeState();
        const add = Array.isArray(rows) ? rows : [];
        const s = await saveScrapeState({ rows: cur.rows.concat(add) });
        sendResponse({ ok: true, state: s, appended: add.length });
        return;
      }

      if (msg.type === "BG_LOG_GET") {
        const data = await chrome.storage.local.get(STORAGE_BG_LOG);
        const lines = Array.isArray(data[STORAGE_BG_LOG]) ? data[STORAGE_BG_LOG] : [];
        sendResponse({ ok: true, lines });
        return;
      }

      if (msg.type === "BG_LOG_CLEAR") {
        await chrome.storage.local.set({ [STORAGE_BG_LOG]: [] });
        sendResponse({ ok: true });
        return;
      }

      if (msg.type === "NET_LISTEN_FLEXPRICER_START") {
        const tabId = sender && sender.tab ? sender.tab.id : null;
        if (typeof tabId !== "number") {
          sendResponse({ ok: false, error: "no_sender_tab" });
          return;
        }
        await detachDebugger(tabId);
        netWatchByTab.set(tabKey(tabId), {
          targetUrl: FLEX_PRICER_URL,
          captured: false,
          startedAt: Date.now(),
          pendingRequestId: "",
          pendingUrl: "",
        });
        await ensureDebuggerAttached(tabId);
        await bgLog("已开启 Network 监听", tabId, { targetUrl: FLEX_PRICER_URL });
        sendResponse({ ok: true, tabId, targetUrl: FLEX_PRICER_URL });
        return;
      }

      if (msg.type === "NET_LISTEN_FLEXPRICER_STOP") {
        const tabId = sender && sender.tab ? sender.tab.id : null;
        if (typeof tabId !== "number") {
          sendResponse({ ok: false, error: "no_sender_tab" });
          return;
        }
        await detachDebugger(tabId);
        netWatchByTab.delete(tabKey(tabId));
        await bgLog("已关闭 Network 监听", tabId);
        sendResponse({ ok: true, tabId });
        return;
      }

      if (msg.type === "RESET_FLOW") {
        await saveFlowState(defaultFlowState());
        sendResponse({ ok: true, state: await loadFlowState() });
        return;
      }

      if (msg.type === "SAVE_PANEL_POSITION") {
        const { left, top } = msg.payload || {};
        await chrome.storage.local.set({
          [STORAGE_PANEL_POS]: { left, top },
        });
        sendResponse({ ok: true });
        return;
      }

      if (msg.type === "SAVE_PANEL_COLLAPSED") {
        const { collapsed } = msg.payload || {};
        await chrome.storage.local.set({
          [STORAGE_PANEL_COLLAPSED]: !!collapsed,
        });
        sendResponse({ ok: true });
        return;
      }

      sendResponse({ ok: false, error: "unknown_message" });
    } catch (e) {
      sendResponse({ ok: false, error: String(e) });
    }
  })();
  return true;
});
