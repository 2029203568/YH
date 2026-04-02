#暂时遭到风控导致返回的响应体为对应的拦截页面
import csv
import json
from datetime import datetime, timedelta

import requests
from playwright.sync_api import sync_playwright

SEARCH_API_URL = "https://www.aeroflot.ru/sbe/flight-search/api/web/v1/flight/search"
BASE_SEARCH_PAGE_URL = "https://www.aeroflot.ru/ru-ru/sb/search/"


def get_user_routes() -> list[dict]:
    print("请输入两段航线信息（回车使用默认值）")

    origin1 = (input("第1段 出发地(IATA) [默认 LED]: ").strip().upper() or "LED")
    destination1 = (input("第1段 目的地(IATA) [默认 ARH]: ").strip().upper() or "ARH")

    default_date1 = "2026-03-31"
    departure_date1 = input(f"第1段 出发日期(YYYY-MM-DD) [默认 {default_date1}]: ").strip() or default_date1

    try:
        dt1 = datetime.strptime(departure_date1, "%Y-%m-%d")
    except ValueError:
        print(f"第1段日期格式错误，已使用默认值 {default_date1}")
        departure_date1 = default_date1
        dt1 = datetime.strptime(default_date1, "%Y-%m-%d")

    default_date2 = (dt1 + timedelta(days=1)).strftime("%Y-%m-%d")

    origin2 = (input(f"第2段 出发地(IATA) [默认 {destination1}]: ").strip().upper() or destination1)
    destination2 = (input(f"第2段 目的地(IATA) [默认 {origin1}]: ").strip().upper() or origin1)
    departure_date2 = input(f"第2段 出发日期(YYYY-MM-DD) [默认 {default_date2}]: ").strip() or default_date2

    try:
        datetime.strptime(departure_date2, "%Y-%m-%d")
    except ValueError:
        print(f"第2段日期格式错误，已使用默认值 {default_date2}")
        departure_date2 = default_date2

    return [
        {"origin": origin1, "destination": destination1, "departure_date": departure_date1},
        {"origin": origin2, "destination": destination2, "departure_date": departure_date2},
    ]


def build_payload(routes: list[dict]) -> dict:
    return {
        "routes": routes,
        "country": "RU",
        "adults": 1,
        "infants": 0,
        "children": 0,
        "childrenfrgn": 0,
        "cabin": "economy",
        "award": False,
        "days": 3,
        "lang": "ru",
        "coupons": [],
        "vouchers": [],
    }


def build_search_page_url(routes: list[dict]) -> str:
    route_parts = [
        f"{r['origin']}.{r['departure_date'].replace('-', '')}.{r['destination']}"
        for r in routes
    ]
    routes_param = "-".join(route_parts)
    return (
        f"{BASE_SEARCH_PAGE_URL}?adults=1&infants=0&children=0&childrenfrgn=0"
        f"&childrenaward=0&cabin=economy&routes={routes_param}"
    )


def get_headers_from_playwright(search_page_url: str) -> tuple[str, dict, str]:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(locale="ru-RU")
        page = context.new_page()

        with page.expect_request(
            lambda req: req.method == "POST" and "/sbe/flight-search/api/web/v1/flight/search" in req.url,
            timeout=30000,
        ) as req_info:
            page.goto(search_page_url, wait_until="domcontentloaded")
            page.wait_for_load_state("networkidle")

        captured_request = req_info.value
        captured_request_url = captured_request.url
        captured_request_headers = dict(captured_request.headers)
        captured_request_body = captured_request.post_data or "{}"

        cookie_header = "; ".join(
            [f"{c['name']}={c['value']}" for c in context.cookies("https://www.aeroflot.ru")]
        )
        if cookie_header:
            captured_request_headers["cookie"] = cookie_header

        browser.close()

    headers = {
        k: v
        for k, v in captured_request_headers.items()
        if k.lower() not in {"content-length", "host"}
    }

    return captured_request_url, headers, captured_request_body


def request_flights_by_protocol(target_url: str, headers: dict, request_body: str) -> requests.Response:
    session = requests.Session()
    response = session.post(target_url, headers=headers, data=request_body, timeout=30)
    response.raise_for_status()
    return response


def clean_search_data(api_json: dict) -> dict:
    data = api_json.get("data", {})
    return {
        "route_itineraries": data.get("route_itineraries", []),
        "route_min_prices": data.get("route_min_prices", []),
        "alternative_destination_available": data.get("alternative_destination_available"),
    }


def export_cleaned_to_csv(cleaned: dict) -> tuple[str, str]:
    itineraries_csv = "cleaned_route_itineraries.csv"
    min_prices_csv = "cleaned_route_min_prices.csv"

    itinerary_headers = [
        "方向序号",
        "行程序号",
        "航段ID",
        "航段时长(分钟)",
        "飞行段序号",
        "出发机场三字码",
        "出发城市",
        "到达机场三字码",
        "到达城市",
        "起飞时间",
        "到达时间",
        "营销航司代码",
        "营销航班号",
        "执飞航司代码",
        "执飞航班号",
        "机型代码",
        "飞行段时长(分钟)",
        "报价ID",
        "总价",
        "币种",
        "舱位代码",
        "品牌代码",
        "余位",
        "可退票",
        "可改期",
    ]

    with open(itineraries_csv, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=itinerary_headers)
        writer.writeheader()

        for direction_index, direction in enumerate(cleaned.get("route_itineraries", [])):
            for itinerary_index, itinerary in enumerate(direction):
                for leg in itinerary.get("legs", []):
                    for segment_index, segment in enumerate(leg.get("segments", [])):
                        for offer in itinerary.get("offers", []):
                            writer.writerow(
                                {
                                    "方向序号": direction_index,
                                    "行程序号": itinerary_index,
                                    "航段ID": leg.get("leg_id"),
                                    "航段时长(分钟)": leg.get("duration_minutes"),
                                    "飞行段序号": segment_index,
                                    "出发机场三字码": segment.get("origin", {}).get("airport_code"),
                                    "出发城市": segment.get("origin", {}).get("city_name"),
                                    "到达机场三字码": segment.get("destination", {}).get("airport_code"),
                                    "到达城市": segment.get("destination", {}).get("city_name"),
                                    "起飞时间": segment.get("departure"),
                                    "到达时间": segment.get("arrival"),
                                    "营销航司代码": segment.get("marketing_airline_code"),
                                    "营销航班号": segment.get("marketing_flight_number"),
                                    "执飞航司代码": segment.get("operating_airline_code"),
                                    "执飞航班号": segment.get("operating_flight_number"),
                                    "机型代码": segment.get("aircraft_type_code"),
                                    "飞行段时长(分钟)": segment.get("duration_minutes"),
                                    "报价ID": offer.get("offer_id"),
                                    "总价": offer.get("price_total_amount"),
                                    "币种": offer.get("currency_code"),
                                    "舱位代码": offer.get("travel_class_code"),
                                    "品牌代码": offer.get("brand_code"),
                                    "余位": offer.get("seat_quantity"),
                                    "可退票": offer.get("refund_allowed"),
                                    "可改期": offer.get("exchange_allowed"),
                                }
                            )

    min_price_headers = ["方向序号", "出发日期", "币种", "最低价格", "舱位"]
    with open(min_prices_csv, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=min_price_headers)
        writer.writeheader()

        for direction_index, direction_prices in enumerate(cleaned.get("route_min_prices", [])):
            for item in direction_prices:
                writer.writerow(
                    {
                        "方向序号": direction_index,
                        "出发日期": item.get("departure_date"),
                        "币种": item.get("currency_code"),
                        "最低价格": item.get("price_amount"),
                        "舱位": item.get("cabin"),
                    }
                )

    return itineraries_csv, min_prices_csv


def main() -> None:
    try:
        routes = get_user_routes()
        payload = build_payload(routes)
        search_page_url = build_search_page_url(routes)

        print("\n[1/3] 使用 Playwright 打开页面并提取真实请求...")
        target_url, headers, request_body = get_headers_from_playwright(search_page_url)

        print(f"已捕获请求 URL: {target_url}")
        print(f"已捕获请求体长度: {len(request_body)}")
        print("[2/3] 使用协议方式重放真实请求...")
        response = request_flights_by_protocol(target_url, headers, request_body)

        print(f"HTTP: {response.status_code}")
        print(f"Content-Type: {response.headers.get('content-type', '')}")

        print("[3/3] 输出协议访问响应...")
        print("Response Headers:")
        print(json.dumps(dict(response.headers), ensure_ascii=False, indent=2))
        print("Response Body:")
        print(response.text)

        # 暂时不做 JSON 解析与 CSV 导出
    except requests.RequestException as exc:
        print(f"请求失败: {exc}")
    except json.JSONDecodeError:
        print("接口返回不是 JSON，可能被风控拦截。")


if __name__ == "__main__":
    main()
