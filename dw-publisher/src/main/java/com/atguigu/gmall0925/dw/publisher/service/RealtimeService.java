package com.atguigu.gmall0925.dw.publisher.service;

import java.util.Map;

public interface RealtimeService {

    public int getDauTotal(String date);

    public Map getDauHours(String date);

    public Map<String,Double> getOrderTotalAmountHours(String date);

    public Double getOrderTotalAmount(String date);

    public Map getSaleDetail(String date ,String keyword,String aggFieldName,int aggSize,int startPage,int pageSize );
    }
