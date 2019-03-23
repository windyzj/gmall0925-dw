package com.atguigu.gmall0925.dw.canal.client;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.gmall0925.dw.canal.handle.CanalHandler;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalClient {

    public static void main(String[] args) {
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop1", 11111), "example", "", "");
        while (true){
            //连接  订阅表
            canalConnector.connect();
            canalConnector.subscribe("gmall0925.order_info");
            Message message = canalConnector.get(100);  //抓取一百个sql命令
            int size = message.getEntries().size();
            if(size==0){
                System.out.println("没有数据");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else{
                List<CanalEntry.Entry> entries = message.getEntries(); //循环处理每个sql命令
                for (CanalEntry.Entry entry : entries) {
                 if(  entry.getEntryType().equals(CanalEntry.EntryType.ROWDATA)){  //只处理 Rowdata类型的命令
                     String tableName = entry.getHeader().getTableName(); //得到表名
                     ByteString storeValue = entry.getStoreValue();//得到数据（序列化）
                     CanalEntry.RowChange rowChange=null;
                     try {
                          rowChange = CanalEntry.RowChange.parseFrom(storeValue);// 把数据反序列化
                     } catch (InvalidProtocolBufferException e) {
                         e.printStackTrace();
                     }
                    // 得到事件类型
                     CanalEntry.EventType eventType = rowChange.getEventType();
                     //得到行的集合
                     List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                     CanalHandler.handle(tableName,eventType,rowDatasList);

                 }


                }


            }


        }


    }
}
