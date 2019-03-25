package com.atguigu.gmall0925.dw.dw2es.app

import com.atguigu.gmall0925.dw.constant.GmallConstants
import com.atguigu.gmall0925.dw.dw2es.bean.SaleDetailDaycount
import com.atguigu.gmall0925.dw.util.MyEsUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object SaleDetailApp {

  def main(args: Array[String]): Unit = {
    var dt=""
    if(args.nonEmpty  &&args(0)!=null){
      dt = args(0)
    }else{
      dt= "2019-03-19"
    }
         val sparkConf: SparkConf = new SparkConf().setAppName("sale_detail").setMaster("local[*]")
         val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
   //查询用户购买明细表
    sparkSession.sql("use gmall0925")
    import  sparkSession.implicits._
    val saleDetailRDD: RDD[SaleDetailDaycount] = sparkSession.sql(s"select user_id,sku_id,user_gender,cast(user_age as int) user_age,user_level,cast(sku_price as double),sku_name,sku_tm_id, sku_category3_id,sku_category2_id,sku_category1_id,sku_category3_name,sku_category2_name,sku_category1_name,spu_id,sku_num,cast(order_count as bigint) order_count,cast(order_amount as double) order_amount,dt " +
      s"from dws_sale_detail_daycount  where dt= '${dt}'").as[SaleDetailDaycount].rdd

    //保存到es 中
    saleDetailRDD.foreachPartition { saleDetailItr =>
      val listBuffer = new  ListBuffer[SaleDetailDaycount]()
      //每100行 执行一次批量保存
      for ( saleDetail <- saleDetailItr ) {
        listBuffer+=saleDetail
        if(listBuffer.size>=100){
          MyEsUtil.executeIndexBulk(GmallConstants.ES_INDEX_SALE,listBuffer.toList,null)
          listBuffer.clear()
        }
      }
      //最后不足100行 执行一次批量保存
      if(listBuffer.size>0){
        MyEsUtil.executeIndexBulk(GmallConstants.ES_INDEX_SALE,listBuffer.toList,null)
      }
    }


  }

}
