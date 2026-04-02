"""
从 AbstractSchedulerService.convertFlightInfoQF / buildFlightInfoQf 迁移的 QF 航班 JSON 清洗。

用法:
  python qf_convert_flight_info.py
      不传参数时在终端交互输入 JSON 路径（可拖拽文件到终端）。
  python qf_convert_flight_info.py [输入.json] [-o 输出前缀] [--no-route-filter]

环境变量:
  EUR_TO_CNY_RATE  对应 Java Redis JQ:rate，默认 5.0（未接 Redis 时的占位）
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple, Union

# ---------------------------------------------------------------------------
# 与 DateUtils.convertAndPrint 一致：毫秒时间戳 → UTC 字符串
# ---------------------------------------------------------------------------


def convert_and_print_ms(timestamp_ms: Union[int, float, str]) -> Optional[str]:
    if timestamp_ms is None:
        return None
    try:
        ts = int(timestamp_ms)
    except (TypeError, ValueError):
        return None
    return datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# 价格：对应 setPriceAndTax（欧元汇率换算人民币，与 Java 一致）
# ---------------------------------------------------------------------------


def set_price_and_tax(price_for_one: Optional[Dict[str, Any]], euro_to_cny_rate: float) -> Tuple[int, int]:
    if not price_for_one:
        return 0, 0
    p = price_for_one.get("priceWithoutTax")
    t = price_for_one.get("tax")
    try:
        p = float(p) if p is not None else 0.0
        t = float(t) if t is not None else 0.0
    except (TypeError, ValueError):
        return 0, 0
    adu_price = int(math.ceil(p * euro_to_cny_rate))
    adu_tax = int(math.ceil(t * euro_to_cny_rate))
    return adu_price, adu_tax


# ---------------------------------------------------------------------------
# fareBasis 清洗：与 Java fareBasis.replace 一致；JSON 中常为数组
# ---------------------------------------------------------------------------


def clean_fare_basis_string(fare_basis: Any) -> str:
    if fare_basis is None:
        return ""
    if isinstance(fare_basis, list):
        fare_basis = fare_basis[0] if fare_basis else ""
    s = str(fare_basis)
    return s.replace("[", "").replace("]", "").replace('"', "")


# ---------------------------------------------------------------------------
# 匹配 listItineraries 中的航段（航班号数字 = segment.fltno）
# ---------------------------------------------------------------------------


def get_flight_segment(
    segment: Dict[str, Any],
    list_itineraries: Optional[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    if not list_itineraries:
        return None
    itineraries = list_itineraries.get("itineraries") or []
    fltno = segment.get("fltno")
    if fltno is None:
        return None
    try:
        target = int(fltno)
    except (TypeError, ValueError):
        return None

    for itin in itineraries:
        for seg in (itin.get("segments") or []):
            fn = seg.get("flightNumber")
            if fn is None or fn == "":
                continue
            try:
                if int(fn) == target:
                    return seg
            except (TypeError, ValueError):
                continue
    return None


def build_flight_info_qf(
    segment: Dict[str, Any],
    recommendation: Dict[str, Any],
    list_itineraries: Optional[Dict[str, Any]],
    euro_to_cny_rate: float,
) -> Dict[str, Any]:
    """对应 buildFlightInfoQf。"""
    cxr = segment.get("cxr") or ""
    fltno = segment.get("fltno", "")
    flight_number = f"{cxr}{fltno}"

    price_for_one = recommendation.get("priceForOne")
    adu_price, adu_tax = set_price_and_tax(price_for_one, euro_to_cny_rate)

    matching = get_flight_segment(segment, list_itineraries)
    dep_time = None
    arr_time = None
    if matching:
        if matching.get("beginDate") is not None:
            dep_time = convert_and_print_ms(matching["beginDate"])
        if matching.get("endDate") is not None:
            arr_time = convert_and_print_ms(matching["endDate"])

    rbds = recommendation.get("rbds") or []
    cabin = rbds[0] if rbds else None
    nb_seats = recommendation.get("nbLastSeatsAvailable")
    if nb_seats is None:
        nb_seats = 9

    return {
        "depAirport": segment.get("dep"),
        "arrAirport": segment.get("arr"),
        "depCity": segment.get("dep"),
        "arrCity": segment.get("arr"),
        "flightNumber": flight_number,
        "aduPrice": adu_price,
        "tax": adu_tax,
        "depTime": dep_time,
        "arrTime": arr_time,
        "transitFlightNumber": "",
        "transitAirport": "",
        "transitCity": "",
        "transitCabin": None,
        "transitDepTime": None,
        "transitArrTime": None,
        "cabin": cabin,
        "seatCount": nb_seats,
        "baggage": "1-23",
        "tuiGaiBZ": "改签费580元 不可退票",
        "yuan": "xinqu",
    }


def _rbds_skip(recommendation: Dict[str, Any]) -> bool:
    """与 Java 一致：rbds 非空且长度不为 1 则 continue。"""
    rbds = recommendation.get("rbds")
    if not rbds:
        return False
    return len(rbds) != 1


def convert_flight_info_qf(
    flight_data: Dict[str, Any],
    euro_to_cny_rate: float,
) -> List[Dict[str, Any]]:
    """
    对应 AbstractSchedulerService.convertFlightInfoQF(FlightData flightData)。
    flight_data 须含 modelInput.segmentAmenitiesKeys 与 modelInput.availability.bounds。
    """
    model_input = flight_data.get("modelInput")
    if not model_input:
        return []

    seg_keys = model_input.get("segmentAmenitiesKeys") or {}
    requests_list = seg_keys.get("routeHappyFrontRequestsPerBound") or []
    availability = model_input.get("availability") or {}
    bounds = availability.get("bounds") or []

    flight_info_list: List[Dict[str, Any]] = []

    for request in requests_list:
        data_obj = request.get("data") or {}
        itineraries = data_obj.get("itineraries") or []
        for itinerary in itineraries:
            for segment in itinerary.get("segments") or []:
                ffc = segment.get("ffc")
                fbc = segment.get("fbc")
                if ffc is None:
                    continue

                for bound in bounds:
                    flights = bound.get("flights") or {}
                    list_itineraries = bound.get("listItineraries")
                    # Map 遍历顺序：按 key 排序以稳定
                    for _fid, flight in sorted(flights.items(), key=lambda x: x[0]):
                        list_recommendation = flight.get("listRecommendation") or {}
                        if ffc not in list_recommendation:
                            continue
                        recommendation = list_recommendation[ffc]
                        if _rbds_skip(recommendation):
                            continue

                        fare_basis_raw = recommendation.get("fareBasis")
                        cleaned = clean_fare_basis_string(fare_basis_raw)
                        if cleaned and cleaned == str(fbc):
                            flight_info = build_flight_info_qf(
                                segment, recommendation, list_itineraries, euro_to_cny_rate
                            )
                            flight_info_list.append(flight_info)
                            break
    return flight_info_list


# ---------------------------------------------------------------------------
# XinQuQFSchedulerServiceImpl.filterRecommendationsByCarrier（可选前置清洗）
# ---------------------------------------------------------------------------


def filter_recommendations_by_carrier(
    flight_data: Dict[str, Any],
    allowed_carriers: Set[str],
    ffc_required: str = "AUAURED1JQ",
) -> Optional[Dict[str, Any]]:
    model_input = flight_data.get("modelInput")
    if not model_input:
        return None
    seg_keys = model_input.get("segmentAmenitiesKeys")
    if not seg_keys:
        return None

    requests_list = seg_keys.get("routeHappyFrontRequestsPerBound") or []
    if not requests_list:
        return None

    for req in requests_list:
        data_obj = req.get("data")
        if not data_obj:
            continue
        itineraries = data_obj.get("itineraries") or []
        if not itineraries:
            continue

        filtered_itineraries: List[Dict[str, Any]] = []
        for itin in itineraries:
            segments = itin.get("segments") or []
            if len(segments) != 1:
                continue
            segment = segments[0] or {}
            cxr = segment.get("cxr")
            ffc = segment.get("ffc")
            if not cxr or cxr not in allowed_carriers:
                continue
            if ffc != ffc_required:
                continue
            filtered_itineraries.append(itin)

        data_obj["itineraries"] = filtered_itineraries

    return flight_data


def load_flight_data_from_file(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        root = json.load(f)
    if "modelInput" in root:
        return {"modelInput": root["modelInput"]}
    return {"modelInput": root}


def _normalize_user_path(raw: str) -> str:
    s = (raw or "").strip().strip('"').strip("'")
    return os.path.expanduser(os.path.expandvars(s))


def prompt_input_json_path() -> str:
    """在终端循环询问，直到得到存在的 JSON 文件路径。"""
    print("QF JSON 清洗：请提供待处理的文件路径（可将文件拖入终端后按回车）。")
    print("直接回车退出。\n")
    while True:
        try:
            line = input("请输入 JSON 文件路径: ")
        except EOFError:
            raise SystemExit(0) from None
        path = _normalize_user_path(line)
        if not path:
            raise SystemExit(0)
        if not os.path.isfile(path):
            print(f"路径无效或不是文件，请重试: {path}\n")
            continue
        if not path.lower().endswith(".json"):
            confirm = input("扩展名不是 .json，仍继续? [y/N]: ").strip().lower()
            if confirm not in ("y", "yes"):
                continue
        return path


def main() -> None:
    parser = argparse.ArgumentParser(description="QF FlightData JSON → FlightInfo 清洗")
    parser.add_argument(
        "input_json",
        nargs="?",
        default=None,
        help="输入 JSON（根级含 modelInput 或整页结构）；省略则在终端交互输入路径",
    )
    parser.add_argument(
        "-o",
        "--output-prefix",
        default="",
        help="输出文件前缀；默认在输入文件同目录生成 *-cleaned-flight_info.json",
    )
    parser.add_argument(
        "--no-route-filter",
        action="store_true",
        help="不进行 filterRecommendationsByCarrier（仅 convertFlightInfoQF）",
    )
    parser.add_argument(
        "--allowed-carriers",
        default="QF",
        help="航段过滤允许的航司，逗号分隔，默认 QF",
    )
    args = parser.parse_args()

    input_path = args.input_json
    if not input_path:
        input_path = prompt_input_json_path()
    else:
        input_path = _normalize_user_path(input_path)
        if not os.path.isfile(input_path):
            print(f"错误: 文件不存在 — {input_path}", file=sys.stderr)
            raise SystemExit(1)

    rate = float(os.environ.get("EUR_TO_CNY_RATE", "5"))

    flight_data = load_flight_data_from_file(input_path)

    if not args.no_route_filter:
        carriers = {x.strip() for x in args.allowed_carriers.split(",") if x.strip()}
        filter_recommendations_by_carrier(flight_data, carriers)

    flight_info_list = convert_flight_info_qf(flight_data, rate)

    base = args.output_prefix
    if not base:
        stem, _ = os.path.splitext(input_path)
        base = stem + "-cleaned"

    out_fi = base + "-flight_info.json"
    out_meta = base + "-meta.json"

    payload = {
        "source": os.path.basename(input_path),
        "route_filter_applied": not args.no_route_filter,
        "allowed_carriers": args.allowed_carriers if not args.no_route_filter else None,
        "euro_to_cny_rate": rate,
        "record_count": len(flight_info_list),
        "flight_info_list": flight_info_list,
    }

    with open(out_fi, "w", encoding="utf-8") as f:
        json.dump(flight_info_list, f, ensure_ascii=False, indent=2)

    with open(out_meta, "w", encoding="utf-8") as f:
        json.dump(
            {k: v for k, v in payload.items() if k != "flight_info_list"},
            f,
            ensure_ascii=False,
            indent=2,
        )

    # 可选：写出裁剪后的 modelInput（体积小，便于复查）
    out_model = base + "-modelInput-filtered.json"
    with open(out_model, "w", encoding="utf-8") as f:
        json.dump(flight_data.get("modelInput", {}), f, ensure_ascii=False, indent=2)

    print(f"写入 {len(flight_info_list)} 条 FlightInfo → {out_fi}")
    print(f"元数据 → {out_meta}")
    print(f"过滤后 modelInput → {out_model}")


if __name__ == "__main__":
    main()
