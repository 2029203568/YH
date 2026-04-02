"""
从 AbstractSchedulerService.convertFlightInfoQF / buildFlightInfoQf 迁移的 QF 航班 JSON 清洗。

按「航程」导出：源 JSON 中 routeHappyFrontRequestsPerBound → data.itineraries 的每一条 itinerary
视为一个航程；清洗后写入独立文件：
  <输入主名>-j001-cleaned-flight_info.json、-j002-...（每条为航班对象数组，共 20 个中文字段，与英文版字段一一对应）

用法:
  python QF_清洗_cn.py              # 交互输入路径
  python QF_清洗_cn.py 源.json [--no-route-filter]

环境变量:
  EUR_TO_CNY_RATE  对应 Java Redis JQ:rate，默认 5.0
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


def fare_basis_codes_from_recommendation(recommendation: Dict[str, Any]) -> Set[str]:
    """recommendation.fareBasis 为字符串或列表时，得到可匹配的 fbc 集合（已清洗）。"""
    raw = recommendation.get("fareBasis")
    out: Set[str] = set()
    if raw is None:
        return out
    if isinstance(raw, list):
        for x in raw:
            c = clean_fare_basis_string(x)
            if c:
                out.add(c)
    else:
        c = clean_fare_basis_string(raw)
        if c:
            out.add(c)
    return out


def recommendation_fbc_matches(recommendation: Dict[str, Any], fbc: Optional[str]) -> bool:
    if fbc is None:
        return False
    codes = fare_basis_codes_from_recommendation(recommendation)
    return str(fbc) in codes


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

    # 20 个中文字段 = 原英文 20 字段（含 transitCabin / transitDepTime / transitArrTime 对应中转舱位等）
    return {
        "成人票面价": adu_price,
        "到达机场": segment.get("arr"),
        "到达城市": segment.get("arr"),
        "到达时间": arr_time,
        "行李额": "1-23",
        "舱位": cabin,
        "出发机场": segment.get("dep"),
        "出发城市": segment.get("dep"),
        "出发时间": dep_time,
        "航班号": flight_number,
        "剩余舱位": nb_seats,
        "税费": adu_tax,
        "中转机场": "",
        "中转城市": "",
        "中转航班号": "",
        "中转舱位": None,
        "中转出发时间": None,
        "中转到达时间": None,
        "退改标识": "改签费580元 不可退票",
        "源": "xinqu",
    }


def _rbds_skip(recommendation: Dict[str, Any]) -> bool:
    """
    Java：rbds 非空且长度不为 1 则跳过。
    联程常见 fareBasis 为多项（每段一个基础运价），此时 rbds 可能多项，不再按单段规则跳过。
    """
    raw_fb = recommendation.get("fareBasis")
    if isinstance(raw_fb, list) and len(raw_fb) > 1:
        return False
    rbds = recommendation.get("rbds")
    if not rbds:
        return False
    return len(rbds) != 1


def convert_flight_info_qf_for_itinerary(
    itinerary: Dict[str, Any],
    bounds: List[Dict[str, Any]],
    euro_to_cny_rate: float,
) -> List[Dict[str, Any]]:
    """
    单条 itinerary（一个航程）内各 segment 依次做与 Java 相同的匹配与 buildFlightInfoQf。
    多段联程则返回多条 FlightInfo（顺序与 segments 一致）。
    """
    flight_info_list: List[Dict[str, Any]] = []
    for segment in itinerary.get("segments") or []:
        ffc = segment.get("ffc")
        fbc = segment.get("fbc")
        if ffc is None:
            continue

        for bound in bounds:
            flights = bound.get("flights") or {}
            list_itineraries = bound.get("listItineraries")
            for _fid, flight in sorted(flights.items(), key=lambda x: x[0]):
                list_recommendation = flight.get("listRecommendation") or {}
                if ffc not in list_recommendation:
                    continue
                recommendation = list_recommendation[ffc]
                if _rbds_skip(recommendation):
                    continue

                if recommendation_fbc_matches(recommendation, fbc):
                    flight_info = build_flight_info_qf(
                        segment, recommendation, list_itineraries, euro_to_cny_rate
                    )
                    flight_info_list.append(flight_info)
                    break
    return flight_info_list


def convert_flight_info_qf(
    flight_data: Dict[str, Any],
    euro_to_cny_rate: float,
) -> List[Dict[str, Any]]:
    """
    对应 AbstractSchedulerService.convertFlightInfoQF：合并所有 itinerary 的结果（扁平列表）。
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
        for itinerary in data_obj.get("itineraries") or []:
            flight_info_list.extend(
                convert_flight_info_qf_for_itinerary(itinerary, bounds, euro_to_cny_rate)
            )
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
            if not segments:
                continue
            ok = True
            for segment in segments:
                seg = segment or {}
                cxr = seg.get("cxr")
                ffc = seg.get("ffc")
                if not cxr or cxr not in allowed_carriers:
                    ok = False
                    break
                if ffc != ffc_required:
                    ok = False
                    break
            if ok:
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


def write_journey_files(
    input_path: str,
    flight_data: Dict[str, Any],
    rate: float,
    output_dir: str,
) -> Tuple[int, List[str]]:
    """
    按 itinerary 逐条写出 *-jNNN-cleaned-flight_info.json，返回 (航程数, 路径列表)。
    """
    model_input = flight_data.get("modelInput") or {}
    seg_keys = model_input.get("segmentAmenitiesKeys") or {}
    requests_list = seg_keys.get("routeHappyFrontRequestsPerBound") or []
    availability = model_input.get("availability") or {}
    bounds = availability.get("bounds") or []

    stem_abs, _ = os.path.splitext(os.path.abspath(input_path))
    base_stem = os.path.basename(stem_abs)
    parent = os.path.dirname(stem_abs)
    out_root = _normalize_user_path(output_dir) if output_dir.strip() else parent
    if output_dir.strip():
        os.makedirs(out_root, exist_ok=True)

    written: List[str] = []
    j = 0
    for request in requests_list:
        data_obj = request.get("data") or {}
        for itinerary in data_obj.get("itineraries") or []:
            infos = convert_flight_info_qf_for_itinerary(itinerary, bounds, rate)
            if not infos:
                continue
            j += 1
            fname = f"{base_stem}-j{j:03d}-cleaned-flight_info.json"
            out_path = os.path.join(out_root, fname)
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(infos, f, ensure_ascii=False, indent=2)
            written.append(out_path)
            print(f"航程 j{j:03d}: {len(infos)} 条航段 → {out_path}")

    return j, written


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
        "--output-dir",
        default="",
        dest="output_dir",
        help="航程 JSON 输出目录（默认与源文件同目录）",
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

    n_journeys, _paths = write_journey_files(input_path, flight_data, rate, args.output_dir)
    if n_journeys == 0:
        print("未生成任何航程文件（无匹配报价或与过滤条件不符）。", file=sys.stderr)
        raise SystemExit(2)
    print(f"共导出 {n_journeys} 个航程 JSON。")


if __name__ == "__main__":
    main()
