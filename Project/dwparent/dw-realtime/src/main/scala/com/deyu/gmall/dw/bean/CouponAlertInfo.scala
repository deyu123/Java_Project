package com.deyu.gmall.dw.bean
import java.util

case class CouponAlertInfo(mid:String,
                           uids:util.HashSet[String],
                           itemIds:util.HashSet[String],
                           events:util.List[String],
                           ts:Long)  {
}



