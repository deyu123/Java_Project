package com.deyu.bigdata.realtime

import com.deyu.sparkmall.common.model.MyKafkaMessage
import com.deyu.sparkmall.common.util.{DateUtil, MyKafkaUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object blacklistRedis {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("blacklistRedis").setMaster("local[*]")
    val context = new StreamingContext(conf, Seconds(5))
    val DStream = MyKafkaUtil.getKafkaStream("ads_log_1", context)
    val kafkaMessage = DStream.map(record => {
      val message = record.value()
      val data = message.split(" ")
      MyKafkaMessage(data(0).toLong, data(1), data(2), data(3), data(4))
    })

    val transDStream: DStream[MyKafkaMessage] = kafkaMessage.transform {
      rdd => {
        // 运行在 Driver 中
        val client = RedisUtil.getJedisClient
        val setTopic = client.smembers("blacklist")
        client.close()
        // java 中的transient 修饰的值transient
        val blacklistbroadcast = context.sparkContext.broadcast(setTopic)
        rdd.filter {
          message => {
            // 运行在Executor
            !blacklistbroadcast.value.contains(message.userid)
          }
        }
      }
    }
    // (data-adv-user, 1)
    val messageMap = transDStream.map {
      message => {
        val date = DateUtil.formatTime(message.timestamp, "yyyy-MM-dd")
        (date + "_" + message.adid + "_" + message.userid, 1L)
      }
    }
    // 阀值的判断
//    val result = messageMap.foreachRDD(rdd => {
//      rdd.foreach {
//        case (key, sum) => {
//          // 向redis 中更新数据
//          val client = RedisUtil.getJedisClient
//          client.hincrBy("data:adv:user:click", key, 1L)
//          // 获取最新的值
//          val value = client.hget("data:adv:user:click", key)
//          if (value.toInt >= 10) {
//            // 拉入黑名单
//            val keys = key.split("_")
//            client.sadd("blacklist", keys(1))
//          }
//          client.close()
//        }
//      }
//    })


    // 使用foreachPartition

    messageMap.foreachRDD(rdd => {
      rdd.foreachPartition(
        data => {
          data.foreach {
            case (key, sum) => {
              // 向redis 中更新数据
              val client = RedisUtil.getJedisClient
              client.hincrBy("data:adv:user:click", key, 1L)
              // 获取最新的值
              val value = client.hget("data:adv:user:click", key)
              if (value.toInt >= 100) {
                // 拉入黑名单
                val keys = key.split("_")
                client.sadd("blacklist", keys(1))
              }
              client.close()
            }
          }
        }
      )
    })

    context.start()
    context.awaitTermination()
  }
}
