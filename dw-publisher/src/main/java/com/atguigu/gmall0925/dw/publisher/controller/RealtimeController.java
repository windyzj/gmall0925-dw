package com.atguigu.gmall0925.dw.publisher.controller;


import com.alibaba.fastjson.JSON;
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
            Date date = null;
            try {
                date = new SimpleDateFormat("yyyy-MM-dd").parse(dateStr);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            Date yesterdayDate = DateUtils.addDays(date, -1);
            String yesterdayString = new SimpleDateFormat("yyyy-MM-dd").format(yesterdayDate);

            Map dauHoursYesterday = realtimeService.getDauHours(yesterdayString);
            housMap.put("yesterday", dauHoursYesterday);

            return JSON.toJSONString(housMap);
        }else{
            return "";
        }
    }


}
