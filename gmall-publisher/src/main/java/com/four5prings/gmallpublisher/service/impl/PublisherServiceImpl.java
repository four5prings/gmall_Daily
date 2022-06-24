package com.four5prings.gmallpublisher.service.impl;

import com.alibaba.fastjson.JSON;
import com.four5prings.gmallpublisher.mapper.DauMapper;
import com.four5prings.gmallpublisher.mapper.OrderMapper;
import com.four5prings.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ClassName PublisherServiceImpl
 * @Description
 * @Author Four5prings
 * @Date 2022/6/23 20:39
 * @Version 1.0
 */
@Service
public class PublisherServiceImpl implements PublisherService {
    @Autowired
    private DauMapper dauMapper;
    @Autowired
    private OrderMapper orderMapper;

    @Override
    public Integer getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauTotalHourMap(String date) {

        List<Map> list = dauMapper.selectDauTotalHourMap(date);

        //创建一个集合存储数据
        HashMap<String, Long> result = new HashMap<>();

        for (Map map : list) {
            result.put((String) map.get("LH"), (Long) map.get("CT"));
        }
        return result;
    }

    @Override
    public Double getOrderAmountTotal(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map getOrderAmountHourMap(String date) {
        List<Map> list = orderMapper.selectOrderAmountHourMap(date);

        //创建map存放数据
        HashMap<String, Double> result = new HashMap<>();

        for (Map map : list) {
            result.put((String) map.get("CREATE_HOUR"), (Double) map.get("SUM_AMOUNT"));
        }
        return result;
    }
}
