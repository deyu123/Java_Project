package com.deyu.bigdata.realtime

import com.deyu.sparkmall.common.model.MyKafkaMessage
import com.deyu.sparkmall.common.util.{DateUtil, MyKafkaUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object adClicks {

  def main(args: Array[String]): Unit = {

    // 需求四 ： 广告黑名单实时统计
    val conf = new SparkConf().setAppName("blacklist").setMaster("local[*]")
    val context = new StreamingContext(conf, Seconds(5))
    val DStream = MyKafkaUtil.getKafkaStream("ads_log_1", context)
    val kafkaMessage = DStream.map(record => {
      val message = record.value()
      val data = message.split(" ")
      MyKafkaMessage(data(0).toLong, data(1), data(2), data(3), data(4))
    })

    // TODO 1. 将数据进行结构的转换 ：(date-area-city-adv, 1)
    val dateAdvUserToCountDStream: DStream[(String, Long)] = kafkaMessage.map(message => {
      val date: String = DateUtil.formatTime(message.timestamp.toLong, "yyyy-MM-dd")
      (date + "_" + message.area + "_" + message.city + "_" + message.adid, 1L)
    })

    val messageSum = dateAdvUserToCountDStream.reduceByKey(_ + _)

    // 聚合后对结果进行判断
    messageSum.foreachRDD(rdd => {
      rdd.foreachPartition(datas => {
        val innerClient = RedisUtil.getJedisClient
        datas.foreach {
          case (key, sum) => {
            innerClient.hincrBy("data:area:city:ads", key, sum)
          }
        }
        innerClient.close()
      })
    })


    context.start()
    context.awaitTermination()
  }
}
