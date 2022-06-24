package com.four5prings.gmallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.four5prings.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName Controller
 * @Description
 * @Author Four5prings
 * @Date 2022/6/23 20:38
 * @Version 1.0
 */
@RestController
public class Controller {

    //自动注入
    @Autowired
    private PublisherService publisherService;

    @RequestMapping("realtime-total")
    public String realtimeTotal(@RequestParam String date) {

        //使用service层的实现类，调用service层的方法，将date参数传递到下一层
        Integer dauTotal = publisherService.getDauTotal(date);
        Double orderAmountTotal = publisherService.getOrderAmountTotal(date);

        /**
         * [{"id":"dau","name":"新增日活","value":1200},
         * {"id":"new_mid","name":"新增设备","value":233}]
         * 获取参数后，如何存储这样一个数据并返回
         * 外层是一个数组，内部是一个k-v类型，我们可以想到使用list+map的方式存储
         * map 的kv的数据类型如何确定呢，key-String，value-Object
         * [{"id":"dau","name":"新增日活","value":1200},
         * {"id":"new_mid","name":"新增设备","value":233 },
         * {"id":"order_amount","name":"新增交易额","value":1000.2 }]
         */
        ArrayList<Map> result = new ArrayList<>();
        HashMap<String, Object> dauMap = new HashMap<>();
        HashMap<String, Object> devMap = new HashMap<>();
        HashMap<String, Object> gmvMap = new HashMap<>();

        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", dauTotal);

        devMap.put("id", "new_mid");
        devMap.put("name", "新增设备");
        devMap.put("value", 233);

        gmvMap.put("id", "order_amount");
        gmvMap.put("name", "新增交易额");
        gmvMap.put("value", orderAmountTotal);
        //将map存入list中
        result.add(dauMap);
        result.add(devMap);
        result.add(gmvMap);
        //这里使用alibaba的json解析类解析
        return JSONObject.toJSONString(result);
    }

    @RequestMapping("realtime-hours")
    public String realtimeHours(
            @RequestParam String id,
            @RequestParam String date) {
        //获取前一天的数据
        String yesterday = LocalDate.parse(date).plusDays(-1).toString();
        Map todayMap = null;
        Map yesterdayMap = null;

        if ("dau".equals(id)) {
            todayMap = publisherService.getDauTotalHourMap(date);
            yesterdayMap = publisherService.getDauTotalHourMap(yesterday);

        } else if ("order_amount".equals(id)) {
            todayMap = publisherService.getOrderAmountHourMap(date);
            yesterdayMap = publisherService.getOrderAmountHourMap(yesterday);
        }

        /**
         * {"yesterday":{"11":383,"12":123,"17":88,"19":200 },
         * "today":{"12":38,"13":1233,"17":123,"19":688 }}
         * 分析数据类型，使用map集合存储，k-String，value-map
         *
         * {"yesterday":{"11":383,"12":123,"17":88,"19":200 },
         * "today":{"12":38,"13":1233,"17":123,"19":688 }}
         */
        HashMap<String, Map> result = new HashMap<>();
        result.put("today", todayMap);
        result.put("yesterday", yesterdayMap);

        return JSON.toJSONString(result);
    }
}
