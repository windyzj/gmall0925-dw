package com.atguigu.gmall0925.dw.bean

case class StartUpLog(mid:String,
                      uid:String,
                      appid:String,
                      area:String,
                      os:String,
                      ch:String,
                      logType:String,
                      vs:String,
                      var logDate:String,   //年-月-日
                      var logHour:String,   //小时数
                      var logHourMinute:String,  //小时：分钟
                      var ts:Long
                     ) {

}
