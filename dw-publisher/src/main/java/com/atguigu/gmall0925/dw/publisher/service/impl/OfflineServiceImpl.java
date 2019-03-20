package com.atguigu.gmall0925.dw.publisher.service.impl;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall0925.dw.publisher.bean.RegionTop3Sku;
import com.atguigu.gmall0925.dw.publisher.mapper.RegionTop3skuMapper;
import com.atguigu.gmall0925.dw.publisher.service.OfflineService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import tk.mybatis.mapper.entity.Example;

import java.util.List;

@Service
public class OfflineServiceImpl  implements OfflineService{

    @Autowired
    RegionTop3skuMapper regionTop3skuMapper;

    @Override
    public List<RegionTop3Sku> getRegionTop3SkuList(String date) {

        List<RegionTop3Sku> regionTop3Skus = regionTop3skuMapper.selectAll();   //查询全部
        System.out.println("all:"+ JSON.toJSONString(regionTop3Skus));

        RegionTop3Sku regionTop3SkuParam=new RegionTop3Sku();   //根据字段值匹配
        regionTop3SkuParam.setDt(date);
        List<RegionTop3Sku> regionTop3SkuListWithParam = regionTop3skuMapper.select(regionTop3SkuParam);
        System.out.println("param:"+ JSON.toJSONString(regionTop3SkuListWithParam));

        Example example=new Example(RegionTop3Sku.class);    //自定义组合查询
        example.createCriteria().andLike("skuName","%小米%");
        List<RegionTop3Sku> regionTop3SkuListExample = regionTop3skuMapper.selectByExample(example);
        System.out.println("小米:"+ JSON.toJSONString(regionTop3SkuListExample));

        return regionTop3SkuListWithParam;
    }
}
