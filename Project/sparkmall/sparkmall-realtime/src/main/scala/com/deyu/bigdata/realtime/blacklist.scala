package com.deyu.bigdata.realtime

import com.deyu.sparkmall.common.model.MyKafkaMessage
import com.deyu.sparkmall.common.util.{DateUtil, MyKafkaUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object blacklist {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("blacklist_1").setMaster("local[*]")
    val context = new StreamingContext(conf, Seconds(5))
    val DStream = MyKafkaUtil.getKafkaStream("ads_log_1", context)
    val kafkaMessage = DStream.map(record => {
      val message = record.value()
      val data = message.split(" ")
      MyKafkaMessage(data(0).toLong, data(1), data(2), data(3), data(4))
    })

    // 需求四 ： 广告黑名单实时统计
    // updateStatusByKey 必须开启checkpoint
    context.sparkContext.setCheckpointDir("cp")

    // 得到一个jedis 连接
//    val client = RedisUtil.getJedisClient
//    val setTopic = client.smembers("blacklist")
//    client.close()

    val transDStream: DStream[MyKafkaMessage] = kafkaMessage.transform {
          // 将DStream 中的RDD 进行转换
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



    // 将数据进行转换
    val transDStreamMap = transDStream.map {
      message => {
        val date = DateUtil.formatTime(message.timestamp, "yyyy-MM-dd")
        (date + "_" + message.userid + "_" + message.adid, 1L)
      }
    }


    // 转换后聚合
    val transDStreamKey = transDStreamMap.updateStateByKey[Long] {
      (seq: Seq[Long], buffer: Option[Long]) => {
        val sum = buffer.getOrElse(0L) + seq.sum
        Option(sum)
      }
    }
    // 阀值的判断
    transDStreamKey.foreachRDD(rdd => {
      rdd.foreach{
        case (key , sum) => {
          println(key + "-" + sum)
          if(sum >= 100) {
            // 拉入黑名单
            val keys = key.split("_")
            val client = RedisUtil.getJedisClient
            client.sadd("blacklist", keys(1))
            client.close()
          }
        }
      }
    })
    context.start()
    context.awaitTermination()
  }
}
