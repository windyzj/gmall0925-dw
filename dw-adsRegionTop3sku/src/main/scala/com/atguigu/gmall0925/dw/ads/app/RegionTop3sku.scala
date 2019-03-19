package com.atguigu.gmall0925.dw.ads.app

import com.atguigu.gmall0925.dw.ads.udaf.ProvinceRemark
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object RegionTop3sku {

  def main(args: Array[String]): Unit = {
    var doDate=""
    if(args.length>0){
        doDate = args(0)
    }
    val sparkConf: SparkConf = new SparkConf().setAppName("region_top3_sku").setMaster("local[*]")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    sparkSession.udf.register("province_remark",new ProvinceRemark())
    println(s"doDate = ${doDate}")
    sparkSession.sql("use gmall0925")
    sparkSession.sql( s"  select    region_name ,sku_id,sku_name,     sum(order_amount)  order_amount ,    province_remark(province_name, order_amount ) remark  from    dws_province_sku  where  dt='${doDate}'  group by region_name ,sku_id,sku_name     order by region_name,sku_name").createOrReplaceTempView("tmp_region_amount")
    sparkSession.sql(" insert into table ads_region_top3_sku  select    region_name  ,sku_name, order_amount ,remark    from        (select     * ,    rank()over(partition by region_name order by order_amount desc ) rk     from tmp_region_amount   )region_amount_rk  where rk <=3").show(100,false)

  }

}
