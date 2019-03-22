package com.atguigu.gmall0925.dw.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall0925.dw.constant.GmallConstants;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.Random;

@RestController  //==controller+ responsebody

public class LoggerController {

    private static final  org.slf4j.Logger logger = LoggerFactory.getLogger(LoggerController.class) ;


    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;


    @GetMapping("testlog")
    public String logger(){
        return "hello world";
    }

    @PostMapping("log")
    public String  putlog(@RequestParam("log") String log){
        //System.out.println(log);

        //推送kafka    producer
        //增加时间戳 ，日志分流
        JSONObject jsonObject = JSON.parseObject(log);
        jsonObject.put("ts",System.currentTimeMillis()+new Random().nextInt(3600*1000*5));
        if("startup".equals(jsonObject.getString("type"))){ //根据日志里的type字段 来决定发送到哪个主题中
            kafkaTemplate.send(GmallConstants.TOPIC_STARTUP,jsonObject.toJSONString());
        }else{
            kafkaTemplate.send(GmallConstants.TOPIC_EVENT,jsonObject.toJSONString());
        }

         //save mysql


        logger.info(log);
        return "success";
    }
}
