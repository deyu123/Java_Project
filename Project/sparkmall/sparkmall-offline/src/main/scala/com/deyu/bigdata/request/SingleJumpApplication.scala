package com.deyu.bigdata.request

import com.deyu.sparkmall.common.model.UserVisitAction
import com.deyu.sparkmall.common.util.{ConfigUtils, StringUtils}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SingleJumpApplication {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("top10")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    spark.sparkContext.setCheckpointDir("cp")
    // 导入隐式转换
    import spark.implicits._
    // 需求3 ： 页面单跳转化率统计
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

    userVisitActionRDD.checkpoint()

    // TODO: 4.1 从行为表中获取数据（pageid）
    // 分母中的数据
    val pageids = ConfigUtils.getValueFromCondition("targetPageFlow").split(",")

    //
    // TODO: 4.2 对数据进行筛选过滤，保留需要统计的页面数据
    val pageFilter = userVisitActionRDD.filter {
      action => {
        pageids.contains(action.page_id.toString)
      }
    }

    // TODO: 4.3 将页面数据进行结构的转换（pageid, 1）
    val pageMap1 = pageFilter.map {
      x => {
        (x.page_id, 1L)
      }
    }

    // TODO: 4.4 将转换后的数据进行聚合统计（pageid, sum）(分母)
    val result1Map = pageMap1.reduceByKey(_ + _)

    // 转换成map
    val page1Res = result1Map.collect().toMap

    // 得到分子
    // 合理的单跳
    val pageZip = pageids.zip(pageids.tail).map {
      case (pageid1, pageid2) => {
        pageid1 + "_" + pageid2
      }
    }

    // TODO: 4.5 从行为表中获取数据，使用session进行分组（sessionid, Iterator[ (pageid, action_time)  ]）
    val page2Group = userVisitActionRDD.groupBy(x => {
      x.session_id
    })

    // TODO: 4.6 对分组后的数据进行时间排序（升序）
    // TODO: 4.7 将排序后的页面ID，两两结合：（1-2，2-3，3-4）
    val resPage = page2Group.map {
      case (session, datas) => {
        // 按照时间排序
        val actions = datas.toList.sortWith {
          (left, right) => {
            left.action_time < right.action_time
          }
        }
        val pageids = actions.map(action => {
          action.page_id
        })

        // 进行过滤
        val pageZipList = pageids.zip(pageids.tail)
        pageZipList
      }
    }

    // TODO: 4.8 对数据进行筛选过滤（1-2，2-3，3-4，4-5，5-6，6-7）
    val dataFlat = resPage.flatMap(list => list)
    val containPage = dataFlat.filter {
      action => {
        pageZip.contains(action._1 + "_" + action._2)
      }
    }
    // TODO: 4.9 对过滤后的数据进行结构的转换：（pageid1-pageid2, 1）
    val containPageMap = containPage.map{
      case (pageid1, pageid2) => {
        (pageid1 + "_" + pageid2, 1)
      }
    }
    // TODO: 4.10 对转换结构后的数据进行聚合统计：（pageid1-pageid2, sum1）(分子)
    val pageSum2 = containPageMap.reduceByKey(_+_)
    // TODO: 4.11 查询对应的分母数据 （pageid1, sum2）

    // TODO: 4.12 计算转化率 ： sum1 / sum2
    pageSum2.foreach{
      case (id, sum) => {
        // 得到分子
        val f: String = id.split("_")(0)
        val m: Long = page1Res.getOrElse(f.toLong, 1L)
        println(f.toDouble / m)
      }
    }

  }

}
