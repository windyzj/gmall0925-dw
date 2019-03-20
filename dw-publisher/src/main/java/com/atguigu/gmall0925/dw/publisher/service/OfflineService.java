package com.atguigu.gmall0925.dw.publisher.service;

import com.atguigu.gmall0925.dw.publisher.bean.RegionTop3Sku;

import java.util.List;

public interface OfflineService {

    public List<RegionTop3Sku> getRegionTop3SkuList(String date);
}
