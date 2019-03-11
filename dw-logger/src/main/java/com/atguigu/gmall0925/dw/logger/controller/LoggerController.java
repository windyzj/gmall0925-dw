package com.atguigu.gmall0925.dw.logger.controller;

import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

@RestController  //==controller+ responsebody

public class LoggerController {

    private static final  org.slf4j.Logger logger = LoggerFactory.getLogger(LoggerController.class) ;

    @GetMapping("testlog")
    public String logger(){
        return "hello world";
    }

    @PostMapping("log")
    public String  putlog(@RequestParam("log") String log){
        //System.out.println(log);
        logger.info(log);
        return "success";
    }
}
