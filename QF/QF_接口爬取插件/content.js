(function () {
  const PANEL_ID = "floating-console";
  if (document.getElementById(PANEL_ID)) return;

  const QANTAS_HOST = "qantas.com";

  let automationRunning = false;

  function logLine(msg) {
    const el = document.getElementById("fc-log");
    if (!el) return;
    const t = new Date().toISOString().slice(11, 19);
    el.textContent += `[${t}] ${msg}\n`;
    el.scrollTop = el.scrollHeight;
  }

  function sleep(ms) {
    return new Promise((r) => setTimeout(r, ms));
  }

  async function humanPause() {
    // 0.2–0.5s 随机延迟
    await sleep(200 + Math.random() * 300);
  }

  async function humanClick(el) {
    if (!el) throw new Error("元素不存在");
    await humanPause();
    el.scrollIntoView({ block: "center", behavior: "instant" });
    try {
      el.dispatchEvent(new MouseEvent("mouseover", { bubbles: true }));
    } catch (_) {}
    await humanPause();
    el.click();
    await humanPause();
  }

  function isQantasPage() {
    return location.hostname.includes(QANTAS_HOST);
  }

  async function sendBg(type, payload) {
    return chrome.runtime.sendMessage({ type, payload });
  }

  function buildPanel() {
    const wrap = document.createElement("div");
    wrap.id = PANEL_ID;
    wrap.innerHTML = `
      <div id="floating-console-header">
        <span id="floating-console-title">Floating Console</span>
        <div class="fc-header-actions">
          <button type="button" id="fc-collapse" class="secondary" title="折叠/展开">—</button>
        </div>
      </div>
      <div class="fc-body">
        <div class="fc-row">
          <label>出发</label>
          <input id="fc-dep" type="text" placeholder="SYD" maxlength="8" autocomplete="off" />
        </div>
        <div class="fc-row">
          <label>到达</label>
          <input id="fc-arr" type="text" placeholder="MEL" maxlength="8" autocomplete="off" />
        </div>
        <div class="fc-row">
          <label>日期</label>
          <input id="fc-date" type="text" placeholder="2026-04-15" maxlength="10" autocomplete="off" />
        </div>
        <div class="fc-row" style="flex-wrap: wrap;">
          <button type="button" id="fc-start-qantas" style="flex:1">澳航单程</button>
          <button type="button" id="fc-reset" class="secondary">重置</button>
        </div>
        <textarea id="console-input" placeholder="输入 JS 在本页执行"></textarea>
        <div class="fc-row">
          <button type="button" id="console-run" style="flex:1">运行 JS</button>
        </div>
        <div id="fc-log"></div>
      </div>
    `;
    document.body.appendChild(wrap);
    return wrap;
  }

  async function restorePanelChrome(panel) {
    const pos = await new Promise((resolve) => {
      chrome.storage.local.get(["floating_console_position"], (d) =>
        resolve(d.floating_console_position)
      );
    });
    const collapsed = await new Promise((resolve) => {
      chrome.storage.local.get(["floating_console_collapsed"], (d) =>
        resolve(!!d.floating_console_collapsed)
      );
    });

    if (pos && typeof pos.left === "number" && typeof pos.top === "number") {
      panel.style.left = pos.left + "px";
      panel.style.top = pos.top + "px";
      panel.style.right = "auto";
      panel.style.bottom = "auto";
    } else {
      panel.style.right = "20px";
      panel.style.bottom = "20px";
    }

    if (collapsed) panel.classList.add("fc-collapsed");
  }

  function wireDrag(panel) {
    const header = panel.querySelector("#floating-console-header");
    let dragging = false;
    let startX = 0;
    let startY = 0;
    let origLeft = 0;
    let origTop = 0;

    header.addEventListener("mousedown", (e) => {
      if (e.target.closest("button")) return;
      dragging = true;
      const rect = panel.getBoundingClientRect();
      origLeft = rect.left;
      origTop = rect.top;
      startX = e.clientX;
      startY = e.clientY;
      panel.style.right = "auto";
      panel.style.bottom = "auto";
      panel.style.left = origLeft + "px";
      panel.style.top = origTop + "px";
      e.preventDefault();
    });

    window.addEventListener(
      "mousemove",
      (e) => {
        if (!dragging) return;
        const dx = e.clientX - startX;
        const dy = e.clientY - startY;
        panel.style.left = origLeft + dx + "px";
        panel.style.top = origTop + dy + "px";
      },
      true
    );

    window.addEventListener(
      "mouseup",
      async () => {
        if (!dragging) return;
        dragging = false;
        const left = panel.offsetLeft;
        const top = panel.offsetTop;
        await sendBg("SAVE_PANEL_POSITION", { left, top });
      },
      true
    );
  }

  async function toggleCollapse(panel) {
    panel.classList.toggle("fc-collapsed");
    const collapsed = panel.classList.contains("fc-collapsed");
    await sendBg("SAVE_PANEL_COLLAPSED", { collapsed });
  }

  async function dismissOverlays() {
    const names = ["接受所有", "Accept all", "同意", "Allow all"];
    for (const n of names) {
      const buttons = [...document.querySelectorAll("button")];
      const b = buttons.find((x) => new RegExp(n, "i").test(x.textContent || ""));
      if (b) {
        await humanClick(b);
        break;
      }
    }
  }

  async function selectAirportCombobox(inputId, code) {
    const box = document.getElementById(inputId);
    if (!box) throw new Error("找不到 #" + inputId);
    await humanClick(box);
    await humanPause();
    box.focus();
    box.value = "";
    box.dispatchEvent(new Event("input", { bubbles: true }));
    box.value = code;
    box.dispatchEvent(new Event("input", { bubbles: true }));
    await humanPause();
    await sleep(200 + Math.random() * 300);
    const listbox = document.querySelector('[role="listbox"]');
    if (listbox) {
      const opts = listbox.querySelectorAll('[role="option"]');
      let found = null;
      const up = code.toUpperCase();
      opts.forEach((o) => {
        if (!found && (o.textContent || "").toUpperCase().includes(up)) found = o;
      });
      if (found) await humanClick(found);
      else {
        await humanPause();
        box.dispatchEvent(new KeyboardEvent("keydown", { key: "ArrowDown", bubbles: true }));
        await humanPause();
        box.dispatchEvent(new KeyboardEvent("keydown", { key: "Enter", bubbles: true }));
      }
    } else {
      await humanPause();
      box.dispatchEvent(new KeyboardEvent("keydown", { key: "ArrowDown", bubbles: true }));
      await humanPause();
      box.dispatchEvent(new KeyboardEvent("keydown", { key: "Enter", bubbles: true }));
    }
  }

  function findSearchButton() {
    const subs = document.querySelectorAll('button[type="submit"]');
    for (const b of subs) {
      if ((b.textContent || "").includes("搜索")) return b;
    }
    const all = document.querySelectorAll("button");
    for (const b of all) {
      if ((b.textContent || "").includes("搜索航班")) return b;
    }
    return null;
  }

  async function waitFor(sel, timeout = 30000) {
    const t0 = Date.now();
    while (Date.now() - t0 < timeout) {
      const el = document.querySelector(sel);
      if (el) return el;
      await sleep(200 + Math.random() * 300);
    }
    throw new Error("等待超时: " + sel);
  }

  function isVisible(el) {
    if (!el) return false;
    const st = window.getComputedStyle(el);
    if (st.display === "none" || st.visibility === "hidden" || Number(st.opacity) === 0) return false;
    const r = el.getBoundingClientRect();
    return r.width > 0 && r.height > 0;
  }

  /**
   * 搜索后若出现「重要信息 / Bono」页，轮询直至出现「继续」按钮并点击。
   * 若限时内未出现（直接进入结果页等），则跳过，不视为失败。
   */
  async function pollAndClickBonoContinue(maxMs = 120000, intervalMs = 400) {
    // 若已到结果页（grouped/upsell），直接跳过
    if (
      document.querySelector("grouped-avail-upsell") ||
      document.querySelector("upsell-itinerary-avail")
    ) {
      logLine("已在搜索结果页，跳过 Bono「继续」轮询");
      return;
    }
    const t0 = Date.now();
    logLine("轮询 Bono「继续」按钮 (#btn-qf-continue)…");
    while (Date.now() - t0 < maxMs) {
      const btn =
        document.getElementById("btn-qf-continue") ||
        document.querySelector("button.qf-continue.btn-primary");
      if (btn && isVisible(btn) && !btn.disabled) {
        logLine("检测到 Bono 页：先 STEP_OK 再点「继续」（防整页跳转丢步）");
        const adv = await sendBg("STEP_OK", { finishedStep: "bono_continue" });
        if (!adv.ok) throw new Error("无法推进流程（STEP_OK bono_continue 失败）");
        await humanClick(btn);
        return;
      }
      await sleep(intervalMs);
    }
    logLine("未在 " + maxMs / 1000 + "s 内出现 Bono「继续」（可能已直接进入预订/结果），已跳过");
  }

  /** JSON 导出完成后：在 3–5 秒内随机间隔派发 wheel，模拟人工浏览 */
  async function simulateBrowseWheelAfterCapture() {
    const totalMs = 3000 + Math.random() * 2000;
    const t0 = Date.now();
    logLine("接口数据已保存，模拟滚轮浏览约 " + Math.round(totalMs / 1000) + "s");
    while (Date.now() - t0 < totalMs) {
      const deltaY = 60 + Math.random() * 180;
      const cx = Math.floor(window.innerWidth * (0.3 + Math.random() * 0.4));
      const cy = Math.floor(window.innerHeight * (0.25 + Math.random() * 0.5));
      const opts = {
        bubbles: true,
        cancelable: true,
        deltaY,
        deltaMode: WheelEvent.DOM_DELTA_PIXEL,
        clientX: cx,
        clientY: cy,
        view: window,
      };
      try {
        const target = document.scrollingElement || document.documentElement || document.body;
        target.dispatchEvent(new WheelEvent("wheel", opts));
      } catch (_) {}
      await sleep(80 + Math.random() * 180);
    }
  }

  function downloadTextFile(text, filename, mime = "text/plain;charset=utf-8") {
    const blob = new Blob([text], { type: mime });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = filename;
    a.click();
    URL.revokeObjectURL(a.href);
  }

  function downloadJsonText(jsonText, filename) {
    downloadTextFile(jsonText, filename, "application/json;charset=utf-8");
  }

  chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
    (async () => {
      try {
        if (msg && msg.type === "DOWNLOAD_JSON") {
          const { jsonText, filename } = msg.payload || {};
          downloadJsonText(String(jsonText || ""), String(filename || "response.json"));
          logLine("已下载 JSON: " + String(filename || "response.json"));
          sendResponse({ ok: true });
          return;
        }
        if (msg && msg.type === "SIMULATE_BROWSE_SCROLL") {
          simulateBrowseWheelAfterCapture()
            .then(() => logLine("模拟滚轮浏览结束"))
            .catch((e) => logLine("模拟滚轮: " + String(e)));
          sendResponse({ ok: true });
          return;
        }
        if (msg && msg.type === "BG_LOG_APPEND") {
          const line = msg.payload && msg.payload.line ? String(msg.payload.line) : "";
          if (line) logLine("[BG] " + line);
          sendResponse({ ok: true });
          return;
        }
      } catch (e) {
        sendResponse({ ok: false, error: String(e) });
      }
    })();
    return true;
  });

  async function runStep(step, state) {
    logLine("执行步骤: " + step);
    switch (step) {
      case "dismiss":
        await dismissOverlays();
        return;
      case "trip": {
        const toggle = await waitFor("#trip-type-toggle-button");
        await humanClick(toggle);
        await waitFor("#trip-type-menu", 8000);
        const one = await waitFor("#trip-type-item-0");
        await humanClick(one);
        return;
      }
      case "departure":
        await selectAirportCombobox("departurePort-input", state.departure);
        return;
      case "arrival":
        await humanPause();
        await selectAirportCombobox("arrivalPort-input", state.arrival);
        return;
      case "date_open": {
        const btn = await waitFor("#daypicker-button");
        await humanClick(btn);
        await waitFor('[data-testid="dialog-day-picker"]', 15000);
        return;
      }
      case "date_pick": {
        const dialog = document.querySelector('[data-testid="dialog-day-picker"]');
        if (!dialog) throw new Error("日历未打开");
        const day = dialog.querySelector(`[data-testid="${state.travelDate}"]`);
        if (!day) throw new Error("找不到日期 " + state.travelDate);
        await humanClick(day);
        return;
      }
      case "confirm": {
        const c = await waitFor('[data-testid="dialogConfirmation"]', 10000);
        await humanClick(c);
        return;
      }
      case "search": {
        await humanPause();
        let btn = findSearchButton();
        if (!btn) throw new Error("找不到搜索按钮");
        logLine("搜索将整页跳转：先 STEP_OK 再点搜索（防卸载丢步）");
        const adv = await sendBg("STEP_OK", { finishedStep: "search" });
        if (!adv.ok) throw new Error("无法推进流程（STEP_OK search 失败）");
        await humanClick(btn);
        return;
      }
      case "bono_continue":
        // 监听 flexPricer 接口；命中后由后台导出 JSON，并在导出后触发页面滚轮模拟浏览
        try {
          const r = await sendBg("NET_LISTEN_FLEXPRICER_START");
          if (r && r.ok) logLine("已开启接口监听（命中即自动导出 JSON）");
        } catch (_) {}
        await pollAndClickBonoContinue(120000, 400);
        return;
      case "scrape_results":
        // 旧版状态：已不再执行 DOM 爬取，仅接口 + JSON
        logLine("当前为仅接口抓取，跳过 scrape_results");
        return;
      default:
        throw new Error("未知步骤: " + step);
    }
  }

  async function automationLoop() {
    if (automationRunning) return;
    automationRunning = true;
    try {
      while (true) {
        const res = await sendBg("GET_FLOW_STATE");
        if (!res.ok || !res.state) break;
        const { flowStep } = res.state;
        if (flowStep === "idle" || flowStep === "done") break;
        if (flowStep !== "bono_continue" && !isQantasPage()) {
          logLine("流程暂停：请在 https://www.qantas.com/zh-cn 页面继续");
          break;
        }
        try {
          await runStep(flowStep, res.state);
          const ok = await sendBg("STEP_OK", { finishedStep: flowStep });
          if (!ok.ok) break;
          logLine("完成: " + flowStep);
          const st = await sendBg("GET_FLOW_STATE");
          if (st.ok && st.state.flowStep === "idle") {
            logLine("流程结束");
            break;
          }
        } catch (err) {
          logLine("错误: " + (err && err.message ? err.message : err));
          await sendBg("STEP_FAIL", {
            error: String(err && err.message ? err.message : err),
            atStep: flowStep,
          });
          break;
        }
      }
    } finally {
      automationRunning = false;
    }
  }

  async function init() {
    const panel = buildPanel();
    await restorePanelChrome(panel);
    wireDrag(panel);

    const collapseBtn = panel.querySelector("#fc-collapse");
    collapseBtn.addEventListener("click", () => toggleCollapse(panel));

    const ta = panel.querySelector("#console-input");
    const runBtn = panel.querySelector("#console-run");
    runBtn.addEventListener("click", () => {
      try {
        const result = eval(ta.value);
        logLine("JS 结果: " + String(result));
        console.log("浮窗执行结果:", result);
      } catch (err) {
        logLine("JS 错误: " + err);
        console.error("浮窗执行错误:", err);
      }
    });

    panel.querySelector("#fc-start-qantas").addEventListener("click", async () => {
      const departure = (panel.querySelector("#fc-dep").value || "").trim();
      const arrival = (panel.querySelector("#fc-arr").value || "").trim();
      const travelDate = (panel.querySelector("#fc-date").value || "").trim();
      try {
        const start = await sendBg("START_QANTAS_FLOW", {
          departure,
          arrival,
          travelDate,
        });
        if (!start || !start.ok) {
          logLine("启动失败: " + (start && start.error ? start.error : "unknown"));
          return;
        }
        logLine("已启动流程");
        automationLoop();
      } catch (e) {
        logLine("启动异常: " + String(e));
      }
    });

    panel.querySelector("#fc-reset").addEventListener("click", async () => {
      await sendBg("RESET_FLOW");
      logLine("已重置流程状态");
    });

    // 启动时拉取后台历史日志，方便排查监听/下载是否触发
    try {
      const r = await sendBg("BG_LOG_GET");
      if (r && r.ok && Array.isArray(r.lines) && r.lines.length) {
        logLine("已加载后台日志: " + r.lines.length + " 行");
        r.lines.slice(-200).forEach((ln) => logLine("[BG] " + ln));
      }
    } catch (_) {}

    const st = await sendBg("GET_FLOW_STATE");
    if (st.ok && st.state && st.state.flowStep && st.state.flowStep !== "idle") {
      logLine("检测到未完成流程，继续: " + st.state.flowStep);
      automationLoop();
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
