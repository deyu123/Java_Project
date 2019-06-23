package com.deyu.bigdata.realtime

import com.deyu.sparkmall.common.model.MyKafkaMessage
import com.deyu.sparkmall.common.util.{DateUtil, MyKafkaUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object window {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("blacklist").setMaster("local[*]")
    val context = new StreamingContext(conf, Seconds(5))
    val DStream = MyKafkaUtil.getKafkaStream("ads_log", context)
    val kafkaMessage = DStream.map(record => {
      val message = record.value()
      val data = message.split(" ")
      MyKafkaMessage(data(0).toLong, data(1), data(2), data(3), data(4))
    })


    val megWin = kafkaMessage.window(Seconds(60), Seconds(10))
    val megMap = megWin.map {
      data => {
        val dateStr = DateUtil.formatTime(data.timestamp, "yyyy-MM-dd HH:mm:ss")
        val key = dateStr.substring(0, dateStr.length - 1) + "0"
        (key, 1L)
      }
    }
    val megSum = megMap.reduceByKey(_+_)

    val result = megSum.transform {
      data => {
        data.sortByKey()
      }
    }
    result.print()

    context.start()
    context.awaitTermination()
  }
}
