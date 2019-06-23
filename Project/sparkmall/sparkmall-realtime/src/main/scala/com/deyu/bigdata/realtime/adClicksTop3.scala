package com.deyu.bigdata.realtime

import com.deyu.sparkmall.common.model.MyKafkaMessage
import com.deyu.sparkmall.common.util.{DateUtil, MyKafkaUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.jackson.JsonMethods

object adClicksTop3 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("blacklist").setMaster("local[*]")
    val context = new StreamingContext(conf, Seconds(5))
    val DStream = MyKafkaUtil.getKafkaStream("ads_log_1", context)
    val kafkaMessage = DStream.map(record => {
      val message = record.value()
      val data = message.split(" ")
      MyKafkaMessage(data(0).toLong, data(1), data(2), data(3), data(4))
    })

    // 需求六 ： 每天各地区 top3 热门广告
    val messageMap = kafkaMessage.map {
      data => {
        val formatDate = DateUtil.formatTime(data.timestamp, "yyyy-MM-dd")
        (formatDate + ":" + data.area +  ":" + data.adid, 1L)
      }
    }
    // TODO 4. 将转换结构后的数据进行聚合 :  (date-area-adv, totalSum)
    val messageSum = messageMap.reduceByKey(_ + _)

    // TODO 5. 将聚合后的数据进行结构的转换 ： (date-area-adv, totalSum) =>  (date-area, (adv, totalSum))
    val messageMaptoMap = messageSum.map {
      case (key, sum) => {
        val keys = key.split(":")
        (keys(0) + "-" + keys(1), (keys(2), sum))
      }
    }
    // TODO 6. 对转换结构后的数据进行分组
    val messageMapGroup = messageMaptoMap.groupByKey()
    // TODO 7. 对广告数据进行排序，取前三名
    val resultDStream = messageMapGroup.mapValues(datas => {
      datas.toList.sortWith {
        (left, right) => {
          left._2 > right._2
        }
      }.take(3).toMap
    })

    // TODO 8. 将聚合结果保存到redis中
    resultDStream.foreachRDD {
      rdd => {
        rdd.foreachPartition {

          datas => {
            val innerClient = RedisUtil.getJedisClient

            datas.foreach {
              data => {
                val keys = data._1.split("-")
                val date = keys(0)
                val area = keys(1)
                val map = data._2

                // list => json
                // [{}, {}, {}]
                // {"xx":10}
                import org.json4s.JsonDSL._
                val listJson = JsonMethods.compact(JsonMethods.render(map))
                innerClient.hset("top3_ads_per_day:" + date, area, listJson)
              }
            }

            innerClient.close()
          }
        }
      }
    }
    context.start ()
    context.awaitTermination ()
  }
}


