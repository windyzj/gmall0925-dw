package com.atguigu.gmall0925.dw.app

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0925.dw.bean.OrderInfo
import com.atguigu.gmall0925.dw.constant.GmallConstants
import com.atguigu.gmall0925.dw.util.MyEsUtil
import com.atguigu.gmall0925.dw.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object RealtimeOrderLog {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("gmall_order").setMaster("local[*]")
     val ssc = new StreamingContext(new SparkContext(sparkConf),Seconds(5))

    val recordDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.TOPIC_ORDER,ssc)
    val orderInfoDstream: DStream[OrderInfo] = recordDstream.map(_.value()).map { jsonString =>
      val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
      val dateArray: Array[String] = orderInfo.createTime.split(" ")
      val dateString: String = dateArray(0)
      val timeArray: Array[String] = dateArray(1).split(":")
      val hour: String = timeArray(0)
      val minute: String = timeArray(1)
      orderInfo.createDate = dateString
      orderInfo.createHour = hour
      orderInfo.createHourMinute = hour + ":" + minute
      orderInfo
    }


    //把新增订单保存到ES中
    orderInfoDstream.foreachRDD{rdd=>

      rdd.foreachPartition{orderItr=>
        MyEsUtil.executeIndexBulk(GmallConstants.ES_INDEX_ORDER,orderItr.toList,"")
      }


    }

    ssc.start()
    ssc.awaitTermination()

  }

}
