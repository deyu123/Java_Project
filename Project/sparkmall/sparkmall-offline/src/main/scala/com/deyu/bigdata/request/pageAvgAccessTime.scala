package com.deyu.bigdata.request

import com.deyu.sparkmall.common.model.UserVisitAction
import com.deyu.sparkmall.common.util.{ConfigUtils, DateUtil, StringUtils}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object pageAvgAccessTime {
  def main(args: Array[String]): Unit = {
    // 需求八 ： 统计每个页面平均停留时间
    val conf = new SparkConf().setMaster("local[*]").setAppName("pageAvgAccessTime")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    spark.sparkContext.setCheckpointDir("cp")
    // 导入隐式转换
    import spark.implicits._
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

    // TODO 1. 将数据使用session进行分组
    val sessionRDD: RDD[(String, Iterable[UserVisitAction])] = userVisitActionRDD.groupBy(action => {
      action.session_id
    })

    // TODO 2. 将分组后的数据使用时间排序，保证按照页面的真正流转顺序
    val sessionToListRDD: RDD[(String, List[(Long, Long)])] = sessionRDD.mapValues(datas => {
      val actions: List[UserVisitAction] = datas.toList.sortWith {
        (left, right) => {
          left.action_time < right.action_time
        }
      }
      // (A,timeA), (B,timeB), (C,timeC)
      val pageToTimeList: List[(Long, String)] = actions.map(action => {
        (action.page_id, action.action_time)
      })

      // TODO 3. 将页面流转的操作进行拉链处理 : ((A,timeA), (B,timeB)), ((B,timeB), (C,timeC))
      val page1ToPage2ZipList: List[((Long, String), (Long, String))] = pageToTimeList.zip(pageToTimeList.tail)

      // TODO 4. 对拉链后的数据进行计算：(A, (timeB-timeA)), (B, (timeC-timeB))
      page1ToPage2ZipList.map {
        case (page1, page2) => {

          val page1Time = DateUtil.getTimestamp(page1._2, "yyyy-MM-dd HH:mm:ss")
          val page2Time = DateUtil.getTimestamp(page2._2, "yyyy-MM-dd HH:mm:ss")

          (page1._1, (page2Time - page1Time))
        }
      }
    })

    val listRDD: RDD[List[(Long, Long)]] = sessionToListRDD.map {
      case (session, list) => list
    }

    val pageToTimeRDD: RDD[(Long, Long)] = listRDD.flatMap(list => list)
    // TODO 5. 对计算结果进行分组：(A, Iterator[ (timeB-timeA), (timeC-timeA) ])
    val pageGroupRDD: RDD[(Long, Iterable[Long])] = pageToTimeRDD.groupByKey()

    // TODO 6. 获取最终结果：(A, sum/size)
    pageGroupRDD.foreach {
      case (pageid, datas) => {
        println(pageid + "=" + (datas.sum / datas.size))
      }
    }

    // 释放资源
    spark.close()

  }
}
