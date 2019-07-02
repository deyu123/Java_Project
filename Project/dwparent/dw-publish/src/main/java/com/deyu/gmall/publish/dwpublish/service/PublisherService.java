package com.deyu.gmall.publish.dwpublish.service;

import java.util.Map;

public interface PublisherService {

    public Long getDauTotal(String date);

    public Map getDauHour(String date);

    public Double getOrderAmount(String date);

    public Map getOrderAmountHour(String tdate);

    public Map getSaleDetail(String date, String keyword, int startpage, int size);
}


