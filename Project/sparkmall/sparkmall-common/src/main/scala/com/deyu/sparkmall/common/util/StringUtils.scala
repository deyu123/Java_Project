package com.deyu.sparkmall.common.util

object StringUtils {

  def isNotEmpty(str:String): Boolean = {
    str != null && !str.equals("")
  }
}
