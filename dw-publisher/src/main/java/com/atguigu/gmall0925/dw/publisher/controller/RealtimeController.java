package com.atguigu.gmall0925.dw.publisher.controller;


import com.alibaba.fastjson.JSON;
import com.atguigu.gmall0925.dw.publisher.bean.Option;
import com.atguigu.gmall0925.dw.publisher.bean.Stat;
import com.atguigu.gmall0925.dw.publisher.service.RealtimeService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.lang.model.element.VariableElement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class RealtimeController {

    @Autowired
    RealtimeService realtimeService;

    @GetMapping("realtime-total")
    public String getRealtimeTotal(@RequestParam("date") String date ){
        List totalList=new ArrayList<Map>();

        int dauTotal = realtimeService.getDauTotal(date);

        Map dauMap=new HashMap();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        dauMap.put("value",dauTotal);
        totalList.add(dauMap);

        Map newMidMap=new HashMap();
        newMidMap.put("id","new_mid");
        newMidMap.put("name","新增用户");
        newMidMap.put("value",3000);
        totalList.add(newMidMap);

        Double orderTotalAmount = realtimeService.getOrderTotalAmount(date);
        Map orderAmountMap=new HashMap();
        orderAmountMap.put("id","order_amount");
        orderAmountMap.put("name","新增收入");
        orderAmountMap.put("value",orderTotalAmount);
        totalList.add(orderAmountMap);

        return JSON.toJSONString(totalList);

    }

    @GetMapping("realtime-hours")
    public  String getRealtimeHour(@RequestParam("id") String id ,@RequestParam("date")String dateStr){
        if("dau".equals(id)) {
            Map housMap = new HashMap();
            //求今天
            Map dauHoursToday = realtimeService.getDauHours(dateStr);
            housMap.put("today", dauHoursToday);

            //求昨天
            String yesterdayString = getYesterday(dateStr);

            Map dauHoursYesterday = realtimeService.getDauHours(yesterdayString);
            housMap.put("yesterday", dauHoursYesterday);

            return JSON.toJSONString(housMap);
        }else if("order_amount".equals(id)){
            Map housMap = new HashMap();
            //今日 收入
            Map<String, Double> orderTotalAmountHoursTdMap = realtimeService.getOrderTotalAmountHours(dateStr);
            //求昨天
            String yesterdayString = getYesterday(dateStr);
            Map<String, Double> orderTotalAmountHoursYdMap = realtimeService.getOrderTotalAmountHours(yesterdayString);


            housMap.put("today",orderTotalAmountHoursTdMap);
            housMap.put("yesterday",orderTotalAmountHoursYdMap);
            return JSON.toJSONString(housMap);
        }else{
            return "";
        }
    }

    private String getYesterday(String today){
        Date date = null;
        try {
            date = new SimpleDateFormat("yyyy-MM-dd").parse(today);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Date yesterdayDate = DateUtils.addDays(date, -1);
        String yesterdayString = new SimpleDateFormat("yyyy-MM-dd").format(yesterdayDate);
        return  yesterdayString;
    }

    @GetMapping("sale_detail")
    public String getSaleDetail(@RequestParam("keyword") String keyword ,@RequestParam("date")String dateStr,@RequestParam("startpage") int startPage,@RequestParam("size") int pageSize ){
        Map detailGenderMap = realtimeService.getSaleDetail(dateStr, keyword, "user_gender", 2, startPage, pageSize);
         //计算男女比例
        HashMap aggsGenderMap =(HashMap) detailGenderMap.get("aggs");
        Integer total =(Integer) detailGenderMap.get("total");
        Stat genderStat = new Stat();
        //制作男女选项
        List<Option> genderList =new ArrayList<>();
        for (Object o : aggsGenderMap.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            Option option = new Option();
            if(entry.getKey().equals("F")){
                option.setName("女");
                option.setValue(Math.round(((Long) entry.getValue())*1000D/total)/10D);
            }else{
                option.setName("男");
                option.setValue(Math.round(((Long)entry.getValue())*1000D/total)/10D);
            }
            genderList.add(option);
        }
        genderStat.setOptions(genderList);
        genderStat.setTitle("用户性别占比");

        //计算年龄段比例
        Map detailAgeMap = realtimeService.getSaleDetail(dateStr, keyword, "user_age", 100, startPage, pageSize);
        int age_20=0;
        int age20_30=0;
        int age30_=0;
        HashMap ageAggs = (HashMap)detailAgeMap.get("aggs");
        //根据年龄划分年龄段 求各个年龄段个数
        for (Object o : ageAggs.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            Integer age =   Integer.parseInt((String)entry.getKey());
            Long count =  (Long) entry.getValue() ;
            if(age<20){
                age_20+=count;
            }
            else if(age>=30){
                age30_+=count;
            }else{
                age20_30+=count;
            }
        }
        //求年龄段比例
        List<Option> ageOptions=new ArrayList<>();
        Option age_20_Option = new Option();
        age_20_Option.setName("20岁以下");
        age_20_Option.setValue( Math.round(age_20*1000D/total)/10D);
        Option age_20_30_Option = new Option();
        age_20_30_Option.setName("20岁到30岁");
        age_20_30_Option.setValue( Math.round(age20_30*1000D/total)/10D);
        Option age_30_Option = new Option();
        age_30_Option.setName("30岁及30岁以上");
        age_30_Option.setValue( Math.round(age30_*1000D/total)/10D);

        ageOptions.add(age_20_Option);
        ageOptions.add(age_20_30_Option);
        ageOptions.add(age_30_Option);

        Stat ageStat  = new Stat();
        ageStat.setTitle("用户年龄占比");
        ageStat.setOptions(ageOptions);
        //把两个聚合的结果保存到list
        List<Stat> statList=new ArrayList<>();
        statList.add(genderStat);
        statList.add(ageStat);

        //都汇总到一个map中 进行json序列化
        HashMap  saleMap = new HashMap<>();
        saleMap.put("total",total);
        saleMap.put("stat",statList);
        saleMap.put("detail",detailGenderMap.get("detail"));

        return  JSON.toJSONString(saleMap);
    }


}
