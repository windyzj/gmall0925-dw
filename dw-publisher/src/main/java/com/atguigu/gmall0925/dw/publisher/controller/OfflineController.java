package com.atguigu.gmall0925.dw.publisher.controller;


import com.alibaba.fastjson.JSON;
import com.atguigu.gmall0925.dw.publisher.bean.RegionTop3Sku;
import com.atguigu.gmall0925.dw.publisher.service.OfflineService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class OfflineController {

    @Autowired
    OfflineService offlineService;

    @GetMapping("/region-top3-sku/{dt}")
    public String getRegionTop3Sku(@PathVariable("dt") String date){
        List<RegionTop3Sku> regionTop3SkuList = offlineService.getRegionTop3SkuList(date);
        String jsonString = JSON.toJSONString(regionTop3SkuList);
        return jsonString;
    }
}
