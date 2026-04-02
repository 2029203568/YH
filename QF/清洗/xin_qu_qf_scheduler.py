"""
Python 版：新渠 QF 航线报价调度逻辑（对应 XinQuQFSchedulerServiceImpl.scheduleTaskXinQuQF 等）。

未在 Java 片段中出现的父类能力（saveFlightPrice、convertFlightInfoQF、线程池状态日志）
以可覆盖方法/回调形式提供，便于接入你自己的存储与监控。
"""

from __future__ import annotations

import json
import logging
import os
import time
from concurrent.futures import ALL_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Set

import requests

from qf_convert_flight_info import convert_flight_info_qf as convert_flight_info_qf_core

# ---------------------------------------------------------------------------
# 日志
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 配置（对应 xinquConfig、字典接口等）
# ---------------------------------------------------------------------------

class XinQuQFConfig:
    """接口与字典来源；可按环境修改或通过子类覆盖。"""

    def __init__(
        self,
        search_qf_url: str = "https://example.com/xinqu/qf/search",  # 请改为实际 QF 查询地址
        dict_api_base: str = "http://47.111.9.44/dev-api/system/dict/data/allList",
        dict_timeout_sec: float = 5.0,
        request_timeout_sec: float = 60.0,
        batch_size: int = 10,
        batch_timeout_sec: float = 300.0,
        max_retries: int = 3,
        thread_pool_workers: int = 10,
    ) -> None:
        self.search_qf_url = search_qf_url
        self.dict_api_base = dict_api_base
        self.dict_timeout_sec = dict_timeout_sec
        self.request_timeout_sec = request_timeout_sec
        self.batch_size = batch_size
        self.batch_timeout_sec = batch_timeout_sec
        self.max_retries = max_retries
        self.thread_pool_workers = thread_pool_workers

    # 字典类型常量（与 DictConstants 一致时可直接使用）
    XINQU_QF_AIRLINES = "xinqu_qf_airlines"  # 按你方实际 dictType 修改
    XINQU_QF_SEGMENTS_DEP = "xinqu_qf_segments_dep"
    XINQU_QF_SEGMENTS_ARR = "xinqu_qf_segments_arr"


# 与 StrConstants.XIAHUAXIAN 一致：下划线分隔多机场代码
SEGMENT_DELIMITER = "_"


# ---------------------------------------------------------------------------
# 核心业务类
# ---------------------------------------------------------------------------

class XinQuQFSchedulerService:
    """
    调度新渠 QF 报价任务。

    子类可覆盖：
      - convert_flight_info_qf
      - save_flight_price
      - log_thread_pool_status
      - build_http_session（自定义连接池、TLS 等）
    """

    def __init__(
        self,
        config: Optional[XinQuQFConfig] = None,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.config = config or XinQuQFConfig()
        self._session = session

    def build_http_session(self) -> requests.Session:
        s = requests.Session()
        s.headers.update({"Content-Type": "application/json", "Accept": "application/json"})
        return s

    @property
    def session(self) -> requests.Session:
        if self._session is None:
            self._session = self.build_http_session()
        return self._session

    def log_thread_pool_status(self) -> None:
        """对应 Java 的 logThreadPoolStatus；无 Spring 线程池时可打占位日志或接入监控。"""
        logger.debug("线程池状态（占位，可按需接入 metrics）")

    # --- 空实现：与 Java 中未实现的 schedule 方法对应 ---

    def schedule_task_atrip(self) -> None:
        pass

    def schedule_task_xin_qu_jq(self, start_day: int, end_day: int) -> None:
        pass

    def schedule_task_xin_qu_va(self) -> None:
        pass

    def schedule_task_xin_qu_qf(self, start_day: int, end_day: int) -> None:
        """对应 scheduleTaskXinQuQF(int startDya, int endDay)。"""
        logger.info("开始查询新渠航线报价,日期：%s", datetime.now().isoformat(timespec="seconds"))
        self.log_thread_pool_status()

        xin_qu_airlines = self.get_sys_dict_data_list(self.config.XINQU_QF_AIRLINES)
        xin_qu_airlines_dep = self.get_sys_dict_data_list(self.config.XINQU_QF_SEGMENTS_DEP)
        xin_qu_airlines_arr = self.get_sys_dict_data_list(self.config.XINQU_QF_SEGMENTS_ARR)

        if not xin_qu_airlines or not xin_qu_airlines_dep or not xin_qu_airlines_arr:
            logger.error("未找到QF航司数据或航段数据，请检查字典表")
            return

        allowed_carriers = {str(x.get("dictValue") or x.get("dict_value")) for x in xin_qu_airlines}
        allowed_carriers.discard("None")

        batch_size = self.config.batch_size
        workers = self.config.thread_pool_workers

        with ThreadPoolExecutor(max_workers=workers) as executor:
            for day_offset in range(start_day, end_day + 1):
                target = datetime.now() + timedelta(days=day_offset)
                formatted_date = target.strftime("%Y-%m-%d")
                logger.info("开始查询新渠QF航线报价，日期：%s", formatted_date)

                dep_split = (xin_qu_airlines_dep[0].get("dictValue") or xin_qu_airlines_dep[0].get("dict_value") or "").split(
                    SEGMENT_DELIMITER
                )
                arr_split = (xin_qu_airlines_arr[0].get("dictValue") or xin_qu_airlines_arr[0].get("dict_value") or "").split(
                    SEGMENT_DELIMITER
                )

                batch_futures: List[Any] = []
                task_count = 0

                for dep_curr in dep_split:
                    for arr in arr_split:
                        if dep_curr == arr:
                            continue
                        fut = executor.submit(
                            self.send_xin_qu_request_qf,
                            dep_curr,
                            arr,
                            formatted_date,
                            allowed_carriers,
                        )
                        batch_futures.append(fut)
                        task_count += 1

                        if task_count >= batch_size:
                            logger.info("QF批次任务数达到%s，等待当前批次完成", batch_size)
                            self._wait_batch(batch_futures)
                            batch_futures.clear()
                            task_count = 0
                            self.log_thread_pool_status()

                if batch_futures:
                    logger.info("QF处理最后一批剩余任务，数量：%s", len(batch_futures))
                    self._wait_batch(batch_futures)
                    self.log_thread_pool_status()

    def _wait_batch(self, futures: List[Any]) -> None:
        done, not_done = wait(
            futures,
            timeout=self.config.batch_timeout_sec,
            return_when=ALL_COMPLETED,
        )
        for f in done:
            try:
                f.result()
            except Exception as e:
                logger.error("QF处理批次任务异常：%s", e, exc_info=True)
        if not_done:
            logger.error("QF批次超时，未完成：%s 个任务", len(not_done))

    def send_xin_qu_request_qf(
        self,
        dep: str,
        arr: str,
        date: str,
        allowed_carriers: Set[str],
    ) -> None:
        max_retries = self.config.max_retries
        retry_count = 0
        while retry_count < max_retries:
            try:
                body_dict = self.get_xin_qu_request_qf(dep, arr, date)
                request_body = json.dumps(body_dict, ensure_ascii=False)
                logger.info("开始请求新渠QF接口，请求参数：%s", request_body)

                start_time = time.time()
                resp = self.session.post(
                    self.config.search_qf_url,
                    data=request_body.encode("utf-8"),
                    timeout=self.config.request_timeout_sec,
                )
                duration_ms = int((time.time() - start_time) * 1000)
                logger.info(
                    "请求地址：%s，返回状态码：%s，请求返回数据是否成功：%s，新渠QF接口耗时：%sms",
                    self.config.search_qf_url,
                    resp.status_code,
                    resp.ok,
                    duration_ms,
                )

                # 注意：Java 原代码对失败分支多次调用 response.body().string() 会读空流；
                # 这里只读取一次响应体。
                text = resp.text or ""

                if resp.ok:
                    xin_qu_response = json.loads(text) if text else {}
                    self.handle_xin_qu_response_qf(xin_qu_response, allowed_carriers, dep, arr, date)
                else:
                    logger.info("QF请求失败 失败后的响应体：%s", text)
                    if text.strip():
                        try:
                            xin_qu_response = json.loads(text)
                            self.handle_xin_qu_response_qf(xin_qu_response, allowed_carriers, dep, arr, date)
                        except json.JSONDecodeError:
                            self.handle_request_failure_qf(
                                retry_count, max_retries, dep, arr, date, resp.status_code, resp.reason
                            )
                            retry_count += 1
                    else:
                        self.handle_request_failure_qf(
                            retry_count, max_retries, dep, arr, date, resp.status_code, resp.reason
                        )
                        retry_count += 1

            except Exception as e:
                self.handle_request_exception_qf(retry_count, max_retries, dep, arr, date, e)
                retry_count += 1

    def handle_xin_qu_response_qf(
        self,
        xin_qu_response: Dict[str, Any],
        allowed_carriers: Set[str],
        dep: str,
        arr: str,
        date: str,
    ) -> None:
        flight_data = self.get_filter_carrier_xin_qu_qf(xin_qu_response, allowed_carriers)
        logger.info(
            "新渠QF请求成功，航线：%s -> %s，日期：%s，查询过滤后结果：%s",
            dep,
            arr,
            date,
            json.dumps(flight_data, ensure_ascii=False) if flight_data is not None else "null",
        )
        if flight_data is not None:
            flight_info_list = self.convert_flight_info_qf(flight_data)
            if flight_info_list:
                self.save_flight_price(flight_info_list)

    def convert_flight_info_qf(self, flight_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        对应 Java convertFlightInfoQF（实现见 qf_convert_flight_info.convert_flight_info_qf）。
        汇率使用环境变量 EUR_TO_CNY_RATE，与 Java Redis JQ:rate 一致。
        """
        rate = float(os.environ.get("EUR_TO_CNY_RATE", "5"))
        return convert_flight_info_qf_core(flight_data, rate)

    def save_flight_price(self, flight_info_list: List[Dict[str, Any]]) -> None:
        """对应 Java saveFlightPrice(flightInfoListXinQu, client)。"""
        logger.warning("save_flight_price 未实现，请子类覆盖：共 %s 条", len(flight_info_list))

    def handle_request_failure_qf(
        self,
        retry_count: int,
        max_retries: int,
        dep: str,
        arr: str,
        date: str,
        status_code: int,
        message: str,
    ) -> None:
        if retry_count < max_retries:
            logger.warning(
                "新渠QF请求失败，正在重试 %s 次，航线：%s -> %s，日期：%s，状态码：%s，错误信息：%s",
                retry_count,
                dep,
                arr,
                date,
                status_code,
                message,
            )
        else:
            logger.error(
                "新渠QF请求失败，已达最大重试次数，航线：%s -> %s，日期：%s，状态码：%s，错误信息：%s",
                dep,
                arr,
                date,
                status_code,
                message,
            )

    def handle_request_exception_qf(
        self,
        retry_count: int,
        max_retries: int,
        dep: str,
        arr: str,
        date: str,
        exc: BaseException,
    ) -> None:
        if retry_count < max_retries:
            logger.warning(
                "新渠QF请求失败，正在重试 %s 次，航线：%s -> %s，日期：%s，错误信息：%s",
                retry_count,
                dep,
                arr,
                date,
                exc,
            )
        else:
            logger.error(
                "新渠QF请求失败，已达最大重试次数，航线：%s -> %s，日期：%s，错误信息：%s",
                dep,
                arr,
                date,
                exc,
                exc_info=True,
            )

    def get_xin_qu_request_qf(self, dep: str, arr: str, date: str) -> Dict[str, Any]:
        return {
            "authInfo": self.build_auth_info(),
            "payload": self.build_payload_qf(dep, arr, date),
        }

    def build_auth_info(self) -> Dict[str, str]:
        return {
            "userName": "QF_JDA",
            "password": "888888",
            "useIP": "debug@@@",
        }

    def build_payload_qf(self, dep: str, arr: str, date: str) -> Dict[str, Any]:
        return {
            "date": date,
            "dep": dep,
            "arr": arr,
            "adt": 1,
        }

    def get_filter_carrier_xin_qu_qf(
        self,
        xin_qu_response: Dict[str, Any],
        allowed_carriers: Set[str],
    ) -> Optional[Dict[str, Any]]:
        if not xin_qu_response or xin_qu_response.get("data") is None:
            logger.warning("新渠QF响应为空或data内容为空")
            return None
        try:
            data_raw = xin_qu_response.get("data")
            if isinstance(data_raw, dict):
                flight_data = data_raw
            else:
                flight_data = json.loads(str(data_raw))

            if not flight_data:
                logger.warning("解析data字段为FlightData对象失败")
                return None

            filtered = self.filter_recommendations_by_carrier(flight_data, allowed_carriers)
            if filtered is None:
                logger.warning("没有符合条件的航班选项")
                return None
            return filtered
        except Exception as e:
            logger.error("处理新渠响应异常：%s", e, exc_info=True)
        return None

    def filter_recommendations_by_carrier(
        self,
        flight_data: Dict[str, Any],
        allowed_carriers: Set[str],
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
                if ffc != "AUAURED1JQ":
                    continue
                filtered_itineraries.append(itin)

            data_obj["itineraries"] = filtered_itineraries

        return flight_data

    def get_sys_dict_data_list(self, dict_type: str) -> List[Dict[str, Any]]:
        url = f"{self.config.dict_api_base}?dictType={dict_type}"
        try:
            r = self.session.get(url, timeout=self.config.dict_timeout_sec)
            r.raise_for_status()
            data = r.json()
            if isinstance(data, list):
                return data
            if isinstance(data, dict) and "data" in data:
                inner = data["data"]
                return inner if isinstance(inner, list) else []
            return []
        except Exception as e:
            logger.error("获取字典配置失败，dictConstants: %s", dict_type, exc_info=True)
            return []


# ---------------------------------------------------------------------------
# 命令行入口示例
# ---------------------------------------------------------------------------

def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


if __name__ == "__main__":
    _setup_logging()

    # 1. 设置真实 QF 查询 URL（与 Java xinquConfig.getSearchQFUrl() 一致）
    cfg = XinQuQFConfig(
        search_qf_url="https://你的域名/实际路径",
        # 若字典 dictType 与默认不同，可改 XinQuQFConfig 类上的常量或在此处传入自定义 config 子类
    )

    service = XinQuQFSchedulerService(config=cfg)

    # 2. 执行：从今天起第 start_day 天到第 end_day 天（与 Java 一致，含端点）
    # 例如 start_day=0, end_day=7 表示查询未来 8 天
    service.schedule_task_xin_qu_qf(start_day=0, end_day=0)
