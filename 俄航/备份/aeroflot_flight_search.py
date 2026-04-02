import csv
import json
from playwright.sync_api import sync_playwright

SEARCH_API_URL = "https://www.aeroflot.ru/sbe/flight-search/api/web/v1/flight/search"
SEARCH_PAGE_URL = (
    "https://www.aeroflot.ru/ru-ru/sb/search/"
    "?adults=1&infants=0&children=0&childrenfrgn=0&childrenaward=0"
    "&cabin=economy&routes=LED.20260331.ARH-ARH.20260401.LED"
)

PAYLOAD = {
    "routes": [
        {"origin": "LED", "destination": "ARH", "departure_date": "2026-03-31"},
        {"origin": "ARH", "destination": "LED", "departure_date": "2026-04-01"},
    ],
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


def fetch_flights(payload: dict):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(locale="ru-RU")
        page = context.new_page()

        page_response = page.goto(SEARCH_PAGE_URL, wait_until="domcontentloaded")
        page.wait_for_timeout(5000)

        api_response = context.request.post(
            SEARCH_API_URL,
            headers={
                "accept": "application/json",
                "content-type": "application/json",
                "origin": "https://www.aeroflot.ru",
                "referer": SEARCH_PAGE_URL,
                "x-client-type": "web-client",
            },
            data=json.dumps(payload),
        )

        result = {
            "page": {
                "url": SEARCH_PAGE_URL,
                "status": page_response.status if page_response else None,
                "headers": page_response.headers if page_response else {},
                "body": page.content(),
            },
            "api": {
                "url": SEARCH_API_URL,
                "status": api_response.status,
                "headers": api_response.headers,
                "body": api_response.text(),
            },
        }

        browser.close()
        return result


def clean_search_data(api_json: dict) -> dict:
    data = api_json.get("data", {})

    cleaned_route_itineraries = []
    for direction in data.get("route_itineraries", []):
        cleaned_direction = []
        for itinerary in direction:
            cleaned_legs = []
            for leg in itinerary.get("legs", []):
                cleaned_segments = []
                for segment in leg.get("segments", []):
                    cleaned_segments.append(
                        {
                            "origin": {
                                "airport_code": segment.get("origin", {}).get("airport_code"),
                                "airport_name": segment.get("origin", {}).get("airport_name"),
                                "terminal_code": segment.get("origin", {}).get("terminal_code"),
                                "city_code": segment.get("origin", {}).get("city_code"),
                                "city_name": segment.get("origin", {}).get("city_name"),
                                "country_code": segment.get("origin", {}).get("country_code"),
                                "country_name": segment.get("origin", {}).get("country_name"),
                            },
                            "destination": {
                                "airport_code": segment.get("destination", {}).get("airport_code"),
                                "airport_name": segment.get("destination", {}).get("airport_name"),
                                "terminal_code": segment.get("destination", {}).get("terminal_code"),
                                "city_code": segment.get("destination", {}).get("city_code"),
                                "city_name": segment.get("destination", {}).get("city_name"),
                                "country_code": segment.get("destination", {}).get("country_code"),
                                "country_name": segment.get("destination", {}).get("country_name"),
                            },
                            "departure": segment.get("departure"),
                            "arrival": segment.get("arrival"),
                            "departure_utc": segment.get("departure_utc"),
                            "arrival_utc": segment.get("arrival_utc"),
                            "departure_offset": segment.get("departure_offset"),
                            "arrival_offset": segment.get("arrival_offset"),
                            "marketing_airline_code": segment.get("marketing_airline_code"),
                            "marketing_airline_name": segment.get("marketing_airline_name"),
                            "marketing_flight_number": segment.get("marketing_flight_number"),
                            "operating_airline_code": segment.get("operating_airline_code"),
                            "operating_airline_name": segment.get("operating_airline_name"),
                            "operating_flight_number": segment.get("operating_flight_number"),
                            "aircraft_type_code": segment.get("aircraft_type_code"),
                            "aircraft_type_name": segment.get("aircraft_type_name"),
                            "duration_minutes": segment.get("duration_minutes"),
                            "stop_quantity": segment.get("stop_quantity"),
                            "stop_airports": segment.get("stop_airports", []),
                            "meal_types": segment.get("meal_types", []),
                            "transfer_train": segment.get("transfer_train"),
                            "transfer_terminal": segment.get("transfer_terminal"),
                            "transfer_airport": segment.get("transfer_airport"),
                            "transfer_same_terminal": segment.get("transfer_same_terminal"),
                        }
                    )

                cleaned_legs.append(
                    {
                        "duration_minutes": leg.get("duration_minutes"),
                        "leg_id": leg.get("leg_id"),
                        "shuttle": leg.get("shuttle"),
                        "franchise_info": leg.get("franchise_info", []),
                        "segments": cleaned_segments,
                    }
                )

            cleaned_offers = []
            for offer in itinerary.get("offers", []):
                booking_class_code = None
                segment_details = (
                    offer.get("leg_details", [{}])[0].get("segment_details", [])
                    if offer.get("leg_details")
                    else []
                )
                if segment_details:
                    booking_class_code = segment_details[0].get("booking_class_code")

                cleaned_offers.append(
                    {
                        "offer_id": offer.get("offer_id"),
                        "price_total_amount": offer.get("price_total_amount"),
                        "price_base_amount": offer.get("price_base_amount"),
                        "tax_total_amount": offer.get("tax_total_amount"),
                        "taxes": offer.get("taxes", []),
                        "currency_code": offer.get("currency_code"),
                        "price_approximate": offer.get("price_approximate"),
                        "passenger_prices": offer.get("passenger_prices", []),
                        "travel_class_code": offer.get("travel_class_code"),
                        "travel_class_name": offer.get("travel_class_name"),
                        "brand_code": offer.get("brand_code"),
                        "brand_name": offer.get("brand_name"),
                        "seat_quantity": offer.get("seat_quantity"),
                        "max_stay_days": offer.get("max_stay_days"),
                        "bonus_miles_percent": offer.get("bonus_miles_percent"),
                        "refund_allowed": offer.get("refund_allowed"),
                        "exchange_allowed": offer.get("exchange_allowed"),
                        "no_baggage": offer.get("no_baggage"),
                        "baggage_quantity": offer.get("baggage_quantity"),
                        "carry_on_quantity": offer.get("carry_on_quantity"),
                        "baggage_weight_text": offer.get("baggage_weight_text"),
                        "carry_on_weight_text": offer.get("carry_on_weight_text"),
                        "seat_preselection_rule": offer.get("seat_preselection_rule"),
                        "refund_rule": offer.get("refund_rule"),
                        "exchange_rule": offer.get("exchange_rule"),
                        "attention_text": offer.get("attention_text"),
                        "promotion_texts": offer.get("promotion_texts", []),
                        "booking_class_code": booking_class_code,
                        "fare_basis_code": segment_details[0].get("fare_basis_code") if segment_details else None,
                        "itinerary_fare_ids": offer.get("itinerary_fare_ids", []),
                    }
                )

            cleaned_direction.append(
                {
                    "legs": cleaned_legs,
                    "offers": cleaned_offers,
                }
            )

        cleaned_route_itineraries.append(cleaned_direction)

    cleaned_route_min_prices = []
    for direction_min_prices in data.get("route_min_prices", []):
        cleaned_direction_min_prices = []
        for item in direction_min_prices:
            cleaned_direction_min_prices.append(
                {
                    "departure_date": item.get("departure_date"),
                    "currency_code": item.get("currency_code"),
                    "price_amount": item.get("price_amount"),
                    "cabin": item.get("cabin"),
                }
            )
        cleaned_route_min_prices.append(cleaned_direction_min_prices)

    return {
        "route_itineraries": cleaned_route_itineraries,
        "route_min_prices": cleaned_route_min_prices,
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
        "无托运行李",
        "托运行李件数",
        "手提行李件数",
        "预订舱位代码",
        "运价基础代码",
        "提示信息",
    ]

    with open(itineraries_csv, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=itinerary_headers)
        writer.writeheader()

        for direction_index, direction in enumerate(cleaned.get("route_itineraries", [])):
            for itinerary_index, itinerary in enumerate(direction):
                legs = itinerary.get("legs", [])
                offers = itinerary.get("offers", [])

                for leg in legs:
                    segments = leg.get("segments", [])
                    for segment_index, segment in enumerate(segments):
                        for offer in offers:
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
                                    "无托运行李": offer.get("no_baggage"),
                                    "托运行李件数": offer.get("baggage_quantity"),
                                    "手提行李件数": offer.get("carry_on_quantity"),
                                    "预订舱位代码": offer.get("booking_class_code"),
                                    "运价基础代码": offer.get("fare_basis_code"),
                                    "提示信息": offer.get("attention_text"),
                                }
                            )

    min_price_headers = [
        "direction_index",
        "departure_date",
        "currency_code",
        "price_amount",
        "cabin",
    ]

    with open(min_prices_csv, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=min_price_headers)
        writer.writeheader()

        for direction_index, direction_prices in enumerate(cleaned.get("route_min_prices", [])):
            for item in direction_prices:
                writer.writerow(
                    {
                        "direction_index": direction_index,
                        "departure_date": item.get("departure_date"),
                        "currency_code": item.get("currency_code"),
                        "price_amount": item.get("price_amount"),
                        "cabin": item.get("cabin"),
                    }
                )

    return itineraries_csv, min_prices_csv


def export_field_descriptions_csv() -> str:
    description_csv = "cleaned_fields_description.csv"
    headers = ["field_path", "description"]
    rows = [
        {"field_path": "data.route_itineraries", "description": "行程列表，二维数组：第一维是方向（去程/返程），第二维是该方向的行程选项"},
        {"field_path": "data.route_min_prices", "description": "每个方向/日期的最低价格摘要"},
        {"field_path": "data.alternative_destination_available", "description": "是否有替代目的地可选"},
        {"field_path": "route_itineraries[].[].legs", "description": "航段列表，通常一个行程一个直飞航段"},
        {"field_path": "route_itineraries[].[].offers", "description": "该行程下的价格方案列表（不同舱位/规则）"},
        {"field_path": "legs[].duration_minutes", "description": "航段飞行时长（分钟）"},
        {"field_path": "legs[].segments", "description": "航段下的具体飞行段列表"},
        {"field_path": "legs[].leg_id", "description": "航段唯一标识"},
        {"field_path": "legs[].shuttle", "description": "是否摆渡航班"},
        {"field_path": "legs[].franchise_info", "description": "特许经营信息"},
        {"field_path": "segments[].origin / destination", "description": "起降机场/城市/国家信息"},
        {"field_path": "segments[].departure / arrival", "description": "本地起降时间（YYYY-MM-DD HH:MM）"},
        {"field_path": "segments[].departure_utc / arrival_utc", "description": "UTC 起降时间"},
        {"field_path": "segments[].departure_offset / arrival_offset", "description": "本地与 UTC 的时差（分钟）"},
        {"field_path": "segments[].marketing_airline_code/name", "description": "营销航司代码/名称"},
        {"field_path": "segments[].marketing_flight_number", "description": "营销航班号"},
        {"field_path": "segments[].operating_airline_code/name", "description": "实际执飞航司代码/名称"},
        {"field_path": "segments[].operating_flight_number", "description": "实际执飞航班号"},
        {"field_path": "segments[].aircraft_type_code/name", "description": "机型代码/名称"},
        {"field_path": "segments[].duration_minutes", "description": "该段飞行时长（分钟）"},
        {"field_path": "segments[].stop_quantity", "description": "经停次数（0=直飞）"},
        {"field_path": "segments[].stop_airports", "description": "经停机场列表"},
        {"field_path": "segments[].meal_types", "description": "餐食类型"},
        {"field_path": "segments[].transfer_*", "description": "是否涉及中转、换航站楼等"},
        {"field_path": "origin/destination.airport_code", "description": "IATA 机场三字码"},
        {"field_path": "origin/destination.airport_name", "description": "机场名称"},
        {"field_path": "origin/destination.terminal_code", "description": "航站楼代码"},
        {"field_path": "origin/destination.city_code / city_name", "description": "城市代码/名称"},
        {"field_path": "origin/destination.country_code / country_name", "description": "国家代码/名称"},
        {"field_path": "offers[].price_total_amount", "description": "总价（含税）"},
        {"field_path": "offers[].price_base_amount", "description": "基础票价"},
        {"field_path": "offers[].tax_total_amount", "description": "税费总额"},
        {"field_path": "offers[].taxes", "description": "税费明细（代码、名称、金额）"},
        {"field_path": "offers[].currency_code", "description": "货币代码"},
        {"field_path": "offers[].price_approximate", "description": "价格是否为近似值"},
        {"field_path": "offers[].passenger_prices", "description": "按旅客类型拆分价格"},
        {"field_path": "offers[].travel_class_code/name", "description": "舱位代码/名称"},
        {"field_path": "offers[].brand_code/name", "description": "品牌/子舱位"},
        {"field_path": "offers[].seat_quantity", "description": "剩余座位数"},
        {"field_path": "offers[].max_stay_days", "description": "最长停留天数"},
        {"field_path": "offers[].bonus_miles_percent", "description": "奖励里程百分比"},
        {"field_path": "offers[].refund_allowed / exchange_allowed", "description": "是否允许退票/改期"},
        {"field_path": "offers[].no_baggage", "description": "是否不含托运行李"},
        {"field_path": "offers[].baggage_quantity", "description": "免费托运行李件数"},
        {"field_path": "offers[].carry_on_quantity", "description": "免费手提行李件数"},
        {"field_path": "offers[].baggage_weight_text", "description": "托运行李重量说明"},
        {"field_path": "offers[].carry_on_weight_text", "description": "手提行李重量说明"},
        {"field_path": "offers[].seat_preselection_rule", "description": "选座规则"},
        {"field_path": "offers[].refund_rule / exchange_rule", "description": "退改规则文字"},
        {"field_path": "offers[].attention_text", "description": "突出提示"},
        {"field_path": "offers[].promotion_texts", "description": "促销/推荐文字"},
        {"field_path": "offers[].offer_id", "description": "报价方案唯一标识"},
        {"field_path": "offers[].booking_class_code", "description": "预订舱位代码（来自 leg_details.segment_details）"},
        {"field_path": "offers[].fare_basis_code", "description": "运价基础代码"},
        {"field_path": "offers[].itinerary_fare_ids", "description": "关联运价 ID 列表"},
        {"field_path": "route_min_prices[].departure_date", "description": "出发日期"},
        {"field_path": "route_min_prices[].currency_code", "description": "货币"},
        {"field_path": "route_min_prices[].price_amount", "description": "该方向最低价格"},
        {"field_path": "route_min_prices[].cabin", "description": "最低价对应舱位"},
    ]

    with open(description_csv, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)

    return description_csv


def main() -> None:
    try:
        result = fetch_flights(PAYLOAD)

        print("=" * 100)
        print("[页面请求响应]")
        print(f"URL: {result['page']['url']}")
        print(f"HTTP: {result['page']['status']}")
        print("Headers:")
        print(json.dumps(result["page"]["headers"], ensure_ascii=False, indent=2))
        print("Body:")
        print(result["page"]["body"])

        print("=" * 100)
        print("[API 请求响应]")
        print(f"URL: {result['api']['url']}")
        print(f"HTTP: {result['api']['status']}")
        print("Headers:")
        print(json.dumps(result["api"]["headers"], ensure_ascii=False, indent=2))
        print("Body:")
        api_body = result["api"]["body"]

        api_json = None
        try:
            api_json = json.loads(api_body)
            print(json.dumps(api_json, ensure_ascii=False, indent=2))
        except Exception:
            print(api_body)

        print("=" * 100)
        print("[清洗后的 data]")
        if api_json and isinstance(api_json, dict) and "data" in api_json:
            cleaned = clean_search_data(api_json)
            print(json.dumps(cleaned, ensure_ascii=False, indent=2))

            itineraries_csv, min_prices_csv = export_cleaned_to_csv(cleaned)
            description_csv = export_field_descriptions_csv()
            print(f"CSV 已输出: {itineraries_csv}")
            print(f"CSV 已输出: {min_prices_csv}")
            print(f"CSV 已输出: {description_csv}")
        else:
            print("API 响应不是可清洗的 JSON data 结构")

        print("=" * 100)
    except Exception as exc:
        print(f"请求失败: {exc}")


if __name__ == "__main__":
    main()
