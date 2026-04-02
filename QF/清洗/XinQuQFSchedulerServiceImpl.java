package com.ruoyi.systemfz.service.scheduler.impl;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.http.HttpUtil;
import com.alibaba.fastjson2.JSON;
import com.ruoyi.common.constant.DictConstants;
import com.ruoyi.common.constant.StrConstants;
import com.ruoyi.common.core.domain.entity.SysDictData;
import com.ruoyi.systemfz.domain.FlightInfo;
import com.ruoyi.systemfz.domain.xinqu.search.xinqu.qf.*;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Service
public class XinQuQFSchedulerServiceImpl extends AbstractSchedulerService {

    @Override
    public void scheduleTaskAtrip() {

    }

    @Override
    public void scheduleTaskXinQuJQ(int startDya, int endDay) {

    }

    @Override
    public void scheduleTaskXinQuVA() {

    }

    @Override
    public void scheduleTaskXinQuQF(int startDya, int endDay) {
        log.info("开始查询新渠航线报价,日期：{}", DateUtil.now());
        // 记录线程池初始状态
        logThreadPoolStatus();
        List<SysDictData> xinQuAirlines = getSysDictDataList(DictConstants.XINQU_QF_AIRLINES);
        List<SysDictData> xinQuAirlinesDep = getSysDictDataList(DictConstants.XINQU_QF_SEGMENTS_DEP);
        List<SysDictData> xinQuAirlinesArr = getSysDictDataList(DictConstants.XINQU_QF_SEGMENTS_ARR);

        if (CollUtil.isEmpty(xinQuAirlines) || CollUtil.isEmpty(xinQuAirlinesDep) || CollUtil.isEmpty(xinQuAirlinesArr)) {
            log.error("未找到QF航司数据或航段数据，请检查字典表");
            return;
        }
        Set<String> allowedCarriers = xinQuAirlines.stream()
                .map(SysDictData::getDictValue)
                .collect(Collectors.toSet());
        // 创建OkHttpClient实例
        okhttp3.OkHttpClient client = getOkHttpClient();

        for (int i = startDya; i <= endDay; i++) {
            Date targetDate = DateUtil.offsetDay(DateUtil.date(), i);
            String formattedDate = DateUtil.format(targetDate, "yyyy-MM-dd");
            log.info("开始查询新渠QF航线报价，日期：{}", formattedDate);
//            String[] sysDictDataDepSplit = xinQuAirlinesDep.get(0).getDictValue().split(StrConstants.PERMISSION_DELIMETER);
            String[] sysDictDataDepSplit = xinQuAirlinesDep.get(0).getDictValue().split(StrConstants.XIAHUAXIAN);
            String[] sysDictDataArrSplit = xinQuAirlinesArr.get(0).getDictValue().split(StrConstants.XIAHUAXIAN);
            List<CompletableFuture<Void>> batchFutures = new ArrayList<>();
            int batchSize = 10;
            int taskCount = 0;
            for (String depCurr : sysDictDataDepSplit) {
//                for (String arr : sysDictDataArrSplit) {
//                    if (dep.equals(arr)) {
//                        continue;
//                    }
                for (String arr : sysDictDataArrSplit) {
                    if (depCurr.equals(arr)) {
                        continue;
                    }
//                String[] split = depCurr.split(StrConstants.XIAHUAXIAN);
                    CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                        sendXinQuRequestQF(depCurr, arr, formattedDate, client, allowedCarriers);
                    }, executorService);
                    batchFutures.add(future);
                    taskCount++;

                    // 当批次任务数达到batchSize时，等待当前批次完成
                    if (taskCount >= batchSize) {
                        log.info("QF批次任务数达到{}，等待当前批次完成", batchSize);
                        // 并行等待所有任务完成
                        CompletableFuture<Void> allOf = CompletableFuture.allOf(
                                batchFutures.toArray(new CompletableFuture[0])
                        );
                        try {
                            allOf.get(5, TimeUnit.MINUTES);
                        } catch (Exception e) {
                            log.error("QF处理批次任务异常：{}", e.getMessage(), e);
                        }
                        // 清空批次任务列表，准备下一批
                        batchFutures.clear();
                        taskCount = 0;
                        // 记录线程池状态
                        logThreadPoolStatus();
                    }
//                }
                }
            }
            // 处理最后一批剩余的任务
            if (!batchFutures.isEmpty()) {
                log.info("QF处理最后一批剩余任务，数量：{}", batchFutures.size());
                // 并行等待所有任务完成
                CompletableFuture<Void> allOf = CompletableFuture.allOf(
                        batchFutures.toArray(new CompletableFuture[0])
                );
                try {
                    allOf.get(5, TimeUnit.MINUTES);
                } catch (Exception e) {
                    log.error("QF处理最后一批任务异常：{}", e.getMessage(), e);
                }
                // 记录线程池状态
                logThreadPoolStatus();
            }
        }
    }

    private void sendXinQuRequestQF(String dep, String arr, String date, okhttp3.OkHttpClient client, Set<String> allowedCarriers) {
        int maxRetries = 3;
        int retryCount = 0;
        while (retryCount < maxRetries) {
            Response response = null;
            try {
                XinQuRequestQF xinQuRequest = getXinQuRequestQF(dep, arr, date);
                String requestBody = JSON.toJSONString(xinQuRequest);
                // 构建请求
                Request request = getRequestXinQuQF(requestBody);
                log.info("开始请求新渠QF接口，请求参数：{}", requestBody);
                // 发送请求
                long startTime = System.currentTimeMillis();
                response = client.newCall(request).execute();
                long endTime = System.currentTimeMillis();
                long duration = endTime - startTime;
                log.info("请求地址：{}，返回状态码：{}，请求返回数据是否成功：{}，新渠QF接口耗时：{}ms",
                        request.url(), response.code(), response.isSuccessful(), duration);
                if (response.isSuccessful()) {
                    String responseBody = response.body().string();
                    XinQuResponseQF xinQuResponse = JSON.parseObject(responseBody, XinQuResponseQF.class);
                    // 处理响应
                    handleXinQuResponseQF(xinQuResponse, allowedCarriers, client, dep, arr, date);
                } else {
                    log.info("QF请求失败 失败后的响应体：{}", response.body().string());
                    if (StrUtil.isNotBlank(response.body().string())){
                        String responseBody = response.body().string();
                        XinQuResponseQF xinQuResponse = JSON.parseObject(responseBody, XinQuResponseQF.class);
                        // 处理响应
                        handleXinQuResponseQF(xinQuResponse, allowedCarriers, client, dep, arr, date);
                    }else {
                        handleRequestFailureQF(retryCount, maxRetries, dep, arr, date, response.code(), response.message());
                        retryCount++;
                    }
                }
            } catch (Exception e) {
                handleRequestExceptionQF(retryCount, maxRetries, dep, arr, date, e);
                retryCount++;
            } finally {
                if (response != null) {
                    // 关闭响应体，防止连接泄漏
                    response.close();
                }
            }
        }
    }

    private void handleXinQuResponseQF(XinQuResponseQF xinQuResponse, Set<String> allowedCarriers, okhttp3.OkHttpClient client, String dep, String arr, String date) {
        // 转换并且过滤航司
        FlightData flightData = getFilterCarrierXinQuQF(xinQuResponse, allowedCarriers);
        log.info("新渠QF请求成功，航线：{} -> {}，日期：{}，查询过滤后结果：{}", dep, arr, date, JSON.toJSONString(flightData));
        if (null != flightData) {
            // 直接插入数据库
            List<FlightInfo> flightInfoListXinQu = convertFlightInfoQF(flightData);
            if (CollUtil.isNotEmpty(flightInfoListXinQu)) {
                saveFlightPrice(flightInfoListXinQu, client);
            }
        }
    }


    private void handleRequestFailureQF(int retryCount, int maxRetries, String dep, String arr, String date, int statusCode, String message) {
        if (retryCount < maxRetries) {
            log.warn("新渠QF请求失败，正在重试 {} 次，航线：{} -> {}，日期：{}，状态码：{}，错误信息：{}",
                    retryCount, dep, arr, date, statusCode, message);
        } else {
            log.error("新渠QF请求失败，已达最大重试次数，航线：{} -> {}，日期：{}，状态码：{}，错误信息：{}",
                    dep, arr, date, statusCode, message);
        }
    }

    private void handleRequestExceptionQF(int retryCount, int maxRetries, String dep, String arr, String date, Exception e) {
        if (retryCount < maxRetries) {
            log.warn("新渠QF请求失败，正在重试 {} 次，航线：{} -> {}，日期：{}，错误信息：{}",
                    retryCount, dep, arr, date, e.getMessage());
        } else {
            log.error("新渠QF请求失败，已达最大重试次数，航线：{} -> {}，日期：{}，错误信息：{}",
                    dep, arr, date, e.getMessage(), e);
        }
    }

    private XinQuRequestQF getXinQuRequestQF(String dep, String arr, String date) {
        XinQuRequestQF xinQuRequest = new XinQuRequestQF();
        AuthInfo authInfo = buildAuthInfo();
        xinQuRequest.setAuthInfo(authInfo);
        Payload payload = buildPayloadQF(dep, arr, date);
        xinQuRequest.setPayload(payload);
        return xinQuRequest;
    }

    private AuthInfo buildAuthInfo() {
        AuthInfo authInfo = new AuthInfo();
        authInfo.setUserName("QF_JDA");
        authInfo.setPassword("888888");
        authInfo.setUseIP("debug@@@");
        return authInfo;
    }

    private Payload buildPayloadQF(String dep, String arr, String date) {
        Payload payload = new Payload();
        payload.setDate(date);
        payload.setDep(dep);
        payload.setArr(arr);
        payload.setAdt(1);
        return payload;
    }

    private Request getRequestXinQuQF(String requestBody) {
        Request request = new Request.Builder()
                .url(xinquConfig.getSearchQFUrl())
                .post(RequestBody.create(MediaType.parse("application/json"), requestBody))
                .build();
        return request;
    }

    private FlightData getFilterCarrierXinQuQF(XinQuResponseQF xinQuResponse, Set<String> allowedCarriers) {
        // 遍历所有新渠响应
        if (xinQuResponse == null || xinQuResponse.getData() == null) {
            log.warn("新渠QF响应为空或data内容为空");
            return null;
        }
        try {
            String data = xinQuResponse.getData();
            // 解析data字段为FlightData对象
            FlightData flightData = JSON.parseObject(data, FlightData.class);
            if (flightData == null) {
                log.warn("解析data字段为FlightData对象失败");
                return null;
            }

            // 过滤符合条件的航班选项
            FlightData filteredFlightData = filterRecommendationsByCarrier(flightData, allowedCarriers);
            if (filteredFlightData == null) {
                log.warn("没有符合条件的航班选项");
                return null;
            }
            return filteredFlightData;
        } catch (Exception e) {
            log.error("处理新渠响应异常：{}", e.getMessage(), e);
        }
        return null;
    }

    private FlightData filterRecommendationsByCarrier(FlightData flightData, Set<String> allowedCarriers) {
        if (flightData == null || flightData.getModelInput() == null ||
                flightData.getModelInput().getSegmentAmenitiesKeys() == null) {
            return null;
        }

        // 遍历 routeHappyFrontRequestsPerBound
        List<RouteHappyFrontRequest> requests = flightData.getModelInput().getSegmentAmenitiesKeys().getRouteHappyFrontRequestsPerBound();
        if (CollUtil.isEmpty(requests)) {
            return null;
        }

        for (RouteHappyFrontRequest request : requests) {
            if (request.getData() == null || CollUtil.isEmpty(request.getData().getItineraries())) {
                continue;
            }

            // 过滤 itineraries
            List<RouteHappyItinerary> filteredItineraries = new ArrayList<>();
            for (RouteHappyItinerary itinerary : request.getData().getItineraries()) {
                // 检查 segment 数量是否为 1
                if (itinerary.getSegments() == null || itinerary.getSegments().size() != 1) {
                    continue;
                }

                // 检查 segment 的 cxr 是否在 allowedCarriers 中
                RouteHappySegment segment = itinerary.getSegments().get(0);
                if (segment == null || segment.getCxr() == null || !allowedCarriers.contains(segment.getCxr())) {
                    continue;
                }

                // 检查 segment 的 ffc 是否为 AUAURED1JQ
                if (segment.getFfc() == null || (!"AUAURED1JQ".equals(segment.getFfc()))) {
                    continue;
                }

                // 添加符合条件的 itinerary
                filteredItineraries.add(itinerary);
            }

            // 更新过滤后的 itineraries
            request.getData().setItineraries(filteredItineraries);
        }

        return flightData;
    }

    private List<SysDictData> getSysDictDataList(String dictConstants) {
        cn.hutool.http.HttpUtil httpUtil = new cn.hutool.http.HttpUtil();
        String url = "http://47.111.9.44/dev-api/system/dict/data/allList?dictType=" + dictConstants;
        try {
            // 设置5秒超时，防止网络请求卡住整个任务
            String response = HttpUtil.get(url, 5000);
            return JSON.parseArray(response, SysDictData.class);
        } catch (Exception e) {
            log.error("获取字典配置失败，dictConstants: {}", dictConstants, e);
            return new ArrayList<>();
        }
    }
}