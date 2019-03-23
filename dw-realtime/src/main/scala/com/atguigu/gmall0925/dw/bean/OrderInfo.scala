package com.atguigu.gmall0925.dw.bean

case class OrderInfo(
                      area: String,
                      consignee: String,
                      orderComment: String,
                      consigneeTel: String,
                      operateTime: String,
                      orderStatus: String,
                      paymentWay: String,
                      userId: String,
                      imgUrl: String,
                      totalAmount: Double,
                      expireTime: String,
                      deliveryAddress: String,
                      createTime: String,
                      trackingNo: String,
                      parentOrderId: String,
                      outTradeNo: String,
                      id: String,
                      tradeBody: String,
                      var createDate: String,
                      var createHour: String,
                      var createHourMinute: String
                    ) {

}
