package com.atguigu.gmall0925.dw.canal.handle;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.atguigu.gmall0925.dw.canal.util.MyKafkaSender;
import com.atguigu.gmall0925.dw.constant.GmallConstants;
import com.google.common.base.CaseFormat;

import java.util.List;

public class CanalHandler {

    public static void  handle(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList){

         //根据表名和事件类型 决定处理规则
        if("order_info".equals(tableName)&&eventType== CanalEntry.EventType.INSERT&&rowDatasList!=null &&rowDatasList.size()>0){
             //循环每一行
            for (CanalEntry.RowData rowData : rowDatasList) {
                //得到修改后的列集合
                List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
                //循环每一列
                JSONObject jsonObject = new JSONObject();
                for (CanalEntry.Column column : afterColumnsList) {
                    System.out.println(column.getName()+":"+column.getValue());
                    String propertyName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, column.getName());
                    //把数据送入 kafka
                    jsonObject.put(propertyName,column.getValue());
                }
                MyKafkaSender.send(GmallConstants.TOPIC_ORDER,jsonObject.toJSONString());


            }
        }
    }
}
