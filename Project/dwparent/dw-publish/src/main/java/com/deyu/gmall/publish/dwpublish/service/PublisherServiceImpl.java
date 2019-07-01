package com.deyu.gmall.publish.dwpublish.service;

import com.deyu.gmall.publish.dwpublish.mapper.DauMapper;
import com.deyu.gmall.publish.dwpublish.mapper.OrderMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    DauMapper dauMapper;

    @Autowired
    OrderMapper orderMapper;

    @Override
    public Long getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauHour(String date) {
        List<Map> mapList = dauMapper.selectDauHourMap(date);
        Map dauHourMap=new HashMap();
        for (Map map : mapList) {
            dauHourMap.put(map.get("LOGHOUR"), map.get("CT"));
        }
        return dauHourMap;
    }

    @Override
    public Double getOrderAmount(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map getOrderAmountHour(String tdate) {

        List<Map> mapList = orderMapper.selectOrderAmountHourMap(tdate);
        Map orderAmountHour = new HashMap<>();
        for (Map map : mapList) {

            orderAmountHour.put(map.get("CREATE_HOUR"), map.get("SUM_AMOUNT"));
        }

        return orderAmountHour;
    }
}
