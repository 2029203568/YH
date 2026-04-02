"""
使用 Playwright 打开澳航中文站，选择单程、填写起降机场与日期并搜索。
起降机场代码与出发日期在终端交互输入；搜索完成后按回车关闭浏览器。

启动时弱化常见自动化标记，并使用与真实浏览更一致的上下文参数；可选使用本机
Google Chrome 通道以贴近日常安装的浏览器二进制特征。
"""

from __future__ import annotations

import argparse
import asyncio
import random
import re
import sys
from typing import Any

from playwright.async_api import Browser, BrowserContext, async_playwright, Locator, Page

QANTAS_ZH_CN = "https://www.qantas.com/zh-cn"

# 与视口一致的常见桌面分辨率；降低「视口 / screen」与 UA 类检测的简单不一致
_VIEWPORT = {"width": 1920, "height": 1080}
_SCREEN = {"width": 1920, "height": 1080}

# 与 locale=zh-CN、澳航中文站相符的常见 Accept-Language（可按需改地区）
_EXTRA_HEADERS = {
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}

# 在页面任意脚本执行前注入，隐藏多数站点检查的 navigator.webdriver 等信号
_STEALTH_INIT_SCRIPT = r"""
(() => {
  try {
    Object.defineProperty(navigator, "webdriver", {
      get: () => undefined,
      configurable: true,
    });
  } catch (_) {}

  try {
    delete navigator.__proto__.webdriver;
  } catch (_) {}

  if (!window.chrome) {
    window.chrome = {};
  }
  if (!window.chrome.runtime) {
    window.chrome.runtime = {};
  }

  const perms = window.navigator.permissions;
  if (perms && typeof perms.query === "function") {
    const origQuery = perms.query.bind(perms);
    perms.query = (parameters) =>
      parameters && parameters.name === "notifications"
        ? Promise.resolve({ state: Notification.permission })
        : origQuery(parameters);
  }
})();
"""


def _launch_options(headless: bool, use_system_chrome: bool) -> dict[str, Any]:
    opts: dict[str, Any] = {
        "headless": headless,
        # 去掉 Chromium 默认的自动化开关，减轻 AutomationControlled 特征
        "ignore_default_args": ["--enable-automation"],
        "args": [
            "--disable-blink-features=AutomationControlled",
            "--disable-session-crashed-bubble",
            "--disable-restore-session-state",
            "--lang=zh-CN",
        ],
    }
    if use_system_chrome:
        opts["channel"] = "chrome"
    return opts


def _context_options() -> dict[str, Any]:
    return {
        "locale": "zh-CN",
        "timezone_id": "Australia/Sydney",
        "viewport": _VIEWPORT.copy(),
        "screen": _SCREEN.copy(),
        "device_scale_factor": 1,
        "color_scheme": "light",
        "reduced_motion": "no-preference",
        "has_touch": False,
        "is_mobile": False,
        "extra_http_headers": _EXTRA_HEADERS.copy(),
    }


async def _new_stealth_context(browser: Browser) -> BrowserContext:
    context = await browser.new_context(**_context_options())
    await context.add_init_script(_STEALTH_INIT_SCRIPT)
    return context


async def _human_pause() -> None:
    """操作间隔：0.1～0.3 秒随机延迟。"""
    await asyncio.sleep(random.uniform(0.1, 0.3))


async def _human_click(locator: Locator) -> None:
    """模拟人工点击：先行间隔、滚入视区、悬停、短促停顿再带按压时长的点击。"""
    await _human_pause()
    await locator.scroll_into_view_if_needed()
    try:
        await locator.hover(timeout=8000)
    except Exception:
        pass
    await asyncio.sleep(random.uniform(0.05, 0.15))
    await locator.click(delay=random.randint(40, 120))


def _read_nonempty(prompt: str) -> str:
    while True:
        value = input(prompt).strip()
        if value:
            return value
        print("不能为空，请重新输入。")


def _read_date_iso() -> str:
    pattern = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    while True:
        raw = _read_nonempty("出发日期 (YYYY-MM-DD，例如 2026-04-15): ")
        if pattern.match(raw):
            return raw
        print("格式应为 YYYY-MM-DD，请重新输入。")


async def _dismiss_overlays(page: Page) -> None:
    # 常见 Cookie / 同意横幅（若有则尽量关掉，避免因遮罩无法点击）
    for name in ("接受所有", "Accept all", "同意", "Allow all"):
        btn = page.get_by_role("button", name=re.compile(re.escape(name), re.I))
        if await btn.count() > 0:
            try:
                await _human_click(btn.first)
            except Exception:
                pass
            break


async def _select_airport_combobox(page: Page, input_id: str, code: str) -> None:
    box = page.locator(f"#{input_id}")
    await box.wait_for(state="visible", timeout=30000)
    await _human_click(box)
    await _human_pause()
    await box.fill(code)
    await _human_pause()
    await page.wait_for_timeout(600)
    # 等待建议列表出现后确认选择
    listbox = page.locator('[role="listbox"]').filter(has=page.locator('[role="option"]'))
    try:
        await listbox.first.wait_for(state="visible", timeout=8000)
        opt = page.locator('[role="option"]').filter(has_text=re.compile(re.escape(code), re.I)).first
        if await opt.count() > 0:
            await _human_click(opt)
        else:
            await _human_pause()
            await box.press("ArrowDown")
            await _human_pause()
            await box.press("Enter")
    except Exception:
        await _human_pause()
        await box.press("ArrowDown")
        await _human_pause()
        await box.press("Enter")


async def run_flow(headless: bool, use_system_chrome: bool) -> None:
    async with async_playwright() as p:
        launch_opts = _launch_options(headless, use_system_chrome)
        try:
            browser = await p.chromium.launch(**launch_opts)
        except Exception as e:
            if use_system_chrome and "channel" in launch_opts:
                print(
                    "使用本机 Chrome 失败（可能未安装 Google Chrome），已回退到 Playwright Chromium。",
                    e,
                    file=sys.stderr,
                )
                launch_opts.pop("channel", None)
                browser = await p.chromium.launch(**launch_opts)
            else:
                raise

        context = await _new_stealth_context(browser)
        page = await context.new_page()
        page.set_default_timeout(30000)

        await page.goto(QANTAS_ZH_CN, wait_until="domcontentloaded", timeout=60000)
        await _human_pause()
        await _dismiss_overlays(page)

        # 行程类型：打开下拉并选「单程」（trip-type-item-0）
        trip_toggle = page.locator("#trip-type-toggle-button")
        await trip_toggle.wait_for(state="visible")
        await _human_click(trip_toggle)
        await page.locator("#trip-type-menu").wait_for(state="visible")
        await _human_click(page.locator("#trip-type-item-0"))

        # 终端交互：出发、到达、日期（在选好单程之后再输入，与页面操作顺序一致）
        print("请在终端依次输入搜索条件。")
        departure = _read_nonempty("出发机场代码 (例如 SYD): ").upper()
        arrival = _read_nonempty("到达机场代码 (例如 MEL): ").upper()
        travel_date = _read_date_iso()

        await _select_airport_combobox(page, "departurePort-input", departure)
        await _human_pause()
        await _select_airport_combobox(page, "arrivalPort-input", arrival)

        # 日期
        await _human_click(page.locator("#daypicker-button"))
        dialog = page.locator('[data-testid="dialog-day-picker"]')
        await dialog.wait_for(state="visible")

        day = dialog.locator(f'[data-testid="{travel_date}"]')
        await day.wait_for(state="visible", timeout=15000)
        await _human_click(day)

        await _human_click(page.locator('[data-testid="dialogConfirmation"]'))

        # 搜索：优先无障碍名称，其次 type=submit
        search_btn = page.get_by_role("button", name=re.compile("搜索航班"))
        if await search_btn.count() > 0:
            await _human_click(search_btn.first)
        else:
            await _human_click(
                page.locator('button[type="submit"]').filter(has_text=re.compile("搜索"))
            )

        print("已提交搜索。请在浏览器中查看结果。")
        input("按回车关闭浏览器…")
        await browser.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="澳航 zh-cn 单程搜索自动化")
    parser.add_argument(
        "--headless",
        action="store_true",
        help="无头模式（默认显示浏览器窗口）",
    )
    parser.add_argument(
        "--system-chrome",
        action="store_true",
        help="尽量使用本机安装的 Google Chrome（channel=chrome），指纹更接近日常浏览器",
    )
    args = parser.parse_args()
    try:
        asyncio.run(run_flow(args.headless, args.system_chrome))
    except KeyboardInterrupt:
        print("\n已中断。", file=sys.stderr)
        sys.exit(130)


if __name__ == "__main__":
    main()
