package com.deyu.bigdata.request

import java.sql.DriverManager
import java.util.UUID

import com.deyu.sparkmall.common.model.UserVisitAction
import com.deyu.sparkmall.common.util.{ConfigUtils, StringUtils}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

object top10 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("top10")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    // 导入隐式转换
    import spark.implicits._
    // TODO 需求1 ： 获取点击、下单和支付数量排名前 10 的品类

    //    4.1 从Hive表中获取用户行为数据

    spark.sql("use " + ConfigUtils.getValueFromConfig("hive.database"))
    var sql = "select * from user_visit_action where 1 = 1"
    val startDate = ConfigUtils.getValueFromCondition("startDate")
    val endDate = ConfigUtils.getValueFromCondition("endDate")
    // 记得字符串要加上引号
    if (StringUtils.isNotEmpty(startDate)) {
      sql += " and date >= '" + startDate + "'"
    }
    if (StringUtils.isNotEmpty(endDate)) {
      sql += " and date <= '" + endDate + "'"
    }

    val dataFrame: DataFrame = spark.sql(sql)
    // 转换为dataSet
    val ds: Dataset[UserVisitAction] = dataFrame.as[UserVisitAction]
    // 转换为RDD
    val userVisitActionRDD: RDD[UserVisitAction] = ds.rdd

    // 4.2 使用累加器将不同的品类的不同指标数据聚合在一起 ： （K-V）(category-指标, SumCount)
    val accumulator = new categoryCountAccumulator()
    spark.sparkContext.register(accumulator)

    userVisitActionRDD.foreach(action => {
      if (action.click_category_id != -1) {
        accumulator.add(action.click_category_id + "-click")
      } else if (action.order_category_ids != null) {
        val orders = action.order_category_ids.split(",")
        for (id <- orders) {
          accumulator.add(id + "-order")
        }
      } else if (action.pay_category_ids != null) {
        val pays = action.pay_category_ids.split(",")
        for (id <- pays) {
          accumulator.add(id + "-pay")
        }
      }
    })
    // map (20 -click, 192)
    val categoryCountMap: mutable.HashMap[String, Long] = accumulator.value
    //    categoryCountMap.foreach(println)
    //    4.3 将聚合后的结果转化结构：(category-指标, SumCount) (category,(指标, SumCount))
    //    val categoryToCountMap = categoryCountMap.map {
    //      case (key, count) => {
    //        // 相同的key 会覆盖
    //        val keys = key.split("-")
    //        (keys(0), (keys(1), count))
    //      }
    //    }
    // (3,Map(3-order -> 552, 3-pay -> 385, 3-click -> 294))
    val keyGroup = categoryCountMap.groupBy(_._1.split("-")(0))
    val taskid = UUID.randomUUID().toString
    //    4.4 将转换结构后的相同品类的数据分组在一起
    val keyGroupMap = keyGroup.map {
      case (key, map) => {
        // 将结果放入到样例类中
        categoryT10(taskid, key, map.getOrElse(key + "-click", 0), map.getOrElse(key + "-order", 0), map.getOrElse(key + "-pay", 0))
      }
    }
    //    keyGroupMap.foreach(println)

    //      4.5 根据品类的不同指标进行排序（降序）
    val keyGroupMapSort = keyGroupMap.toList.sortWith {
      (left, right) => {
        if (left.click_count > right.click_count) {
          true
        } else if (left.click_count == right.click_count) {
          if (left.order_count > left.order_count) {
            true
          } else if (left.order_count == right.click_count) {
            left.pay_count > right.pay_count
          } else {
            false
          }
        } else {
          false
        }
      }
    }
//    keyGroupMapSort.foreach(println)
    //    4.6 获取排序后的前10名
    val result = keyGroupMapSort.take(10)
    result.foreach(println)
    //      4.7 将结果保存到数据库中
    val driver = ConfigUtils.getValueFromConfig("jdbc.driver.class")
    val url = ConfigUtils.getValueFromConfig("jdbc.url")
    val username = ConfigUtils.getValueFromConfig("jdbc.username")
    val password = ConfigUtils.getValueFromConfig("jdbc.password")

    Class.forName(driver)
    val connection = DriverManager.getConnection(url,username, password)
    val statement = connection.prepareStatement("insert into category_top10 values (?,?,?,?,?)")

    result.foreach(data =>{
      statement.setString(1,data.taskId)
      statement.setString(2, data.category_id)
      statement.setLong(3,data.click_count)
      statement.setLong(4, data.order_count)
      statement.setLong(5, data.pay_count)
      statement.executeUpdate()
    })

    // 释放资源
    statement.close()
    connection.close()
    spark.close()
  }

}

case class categoryT10(taskId: String, category_id: String, click_count: Long, order_count: Long, pay_count: Long)

// 声明累加器
class categoryCountAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Long]] {

  var map = new mutable.HashMap[String, Long]()

  override def isZero: Boolean = {
    map.isEmpty
  }

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
    new categoryCountAccumulator
  }

  override def reset(): Unit = {
    map.clear()
  }

  override def add(v: String): Unit = {
    map(v) = map.getOrElse(v, 0L) + 1
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
    val map1 = map
    val map2 = other.value
    // merge 后必须要有一个
    map = map1.foldLeft(map2) {
      (innerMap, kv) => {
        val k = kv._1
        val v = kv._2
        innerMap(k) = innerMap.getOrElse(k, 0L) + v
        innerMap
      }
    }
  }

  override def value: mutable.HashMap[String, Long] = {
    map
  }
}