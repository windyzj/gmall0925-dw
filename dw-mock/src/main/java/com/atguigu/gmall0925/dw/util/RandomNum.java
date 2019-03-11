package com.atguigu.gmall0925.dw.util;

import java.util.Random;

public class RandomNum {

    public static final  int getRandInt(int fromNum,int toNum){
        return   fromNum+ new Random().nextInt(toNum-fromNum+1);
    }
}