package com.deyu.sparkmall.common.util

import java.util.ResourceBundle

import com.alibaba.fastjson.{JSON, JSONObject}

object ConfigUtils {
  private val rb: ResourceBundle = ResourceBundle.getBundle("config")
  private val condRb: ResourceBundle = ResourceBundle.getBundle("condition")

  /**
    * 从指定的配置文件中查找指定的 key 的值
    *
    */
  def getValueFromConfig(key: String) ={
    rb.getString(key)
  }

  /**
    * 从JSON 文件根据指定的key 获取值
    */

  def getValueFromCondition(cond:String): String = {
    val jsonStr = condRb.getString("condition.params.json")
    val jsonObject: JSONObject = JSON.parseObject(jsonStr)
    jsonObject.getString(cond)
  }

}
