package com.atguigu.gmall0925.dw.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RandomOptionGroup<T> {

    int totalWeight=0;

    List<RanOpt> optList=new ArrayList();

    public   RandomOptionGroup(RanOpt<T>... opts) {
        for (RanOpt opt : opts) {
            totalWeight+=opt.getWeight();
            for (int i = 0; i <opt.getWeight() ; i++) {
                optList.add(opt);
            }

        }
    }

    public RanOpt<T> getRandomOpt() {
        int i = new Random().nextInt(totalWeight);
        return optList.get(i);
    }


    public static void main(String[] args) {
        RanOpt[] opts= {new RanOpt("zhang3",20),new RanOpt("li4",30),new RanOpt("wang5",50)};
        RandomOptionGroup randomOptionGroup = new RandomOptionGroup(opts);
        for (int i = 0; i <10 ; i++) {
            System.out.println(randomOptionGroup.getRandomOpt().getValue());
        }

    }





}
