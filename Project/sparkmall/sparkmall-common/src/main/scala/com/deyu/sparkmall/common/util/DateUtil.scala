package com.deyu.sparkmall.common.util

import java.text.SimpleDateFormat
import java.util.Date

object DateUtil {
  /**
    * 将日期字符串转换成时间戳
    * @param dataString
    * @param f
    * @return
    */
  def getTimestamp(dataString: String, f: String):Long = {
    val sdf = new SimpleDateFormat(f)
    sdf.parse(dataString).getTime
  }

  def formatTime(ts:Long, f: String)  = {
    formatDate(new Date(ts), f)
  }

  def formatDate(d: Date, f : String) = {
    val format = new SimpleDateFormat(f)
    format.format(d)
  }
}
