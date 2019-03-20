package com.atguigu.gmall0925.dw.publisher.bean;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.Table;

@Table(name = "ads_region_top3_sku")
public class RegionTop3Sku {

    @Column
    String dt ;

    @Column
    String region;
    @Column
    String skuName;
    @Column
    Double orderAmount;
    @Column
    String provinceRemark;

    public String getDt() {
        return dt;
    }

    public void setDt(String dt) {
        this.dt = dt;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getSkuName() {
        return skuName;
    }

    public void setSkuName(String skuName) {
        this.skuName = skuName;
    }

    public Double getOrderAmount() {
        return orderAmount;
    }

    public void setOrderAmount(Double orderAmount) {
        this.orderAmount = orderAmount;
    }

    public String getProvinceRemark() {
        return provinceRemark;
    }

    public void setProvinceRemark(String provinceRemark) {
        this.provinceRemark = provinceRemark;
    }


}
