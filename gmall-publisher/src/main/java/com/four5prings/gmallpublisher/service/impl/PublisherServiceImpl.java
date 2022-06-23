package com.four5prings.gmallpublisher.service.impl;

import com.four5prings.gmallpublisher.mapper.DauMapper;
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
}
