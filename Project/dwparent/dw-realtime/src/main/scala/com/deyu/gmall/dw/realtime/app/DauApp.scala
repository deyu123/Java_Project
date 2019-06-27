package com.deyu.gmall.dw.realtime.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.deyu.gmall.dw.bean.StartUpLog
import com.deyu.gmall.dw.dwlogger.constant.GmallConstants
import com.deyu.gmall.dw.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.phoenix.spark._

object DauApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DauApp")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(5))
    // 不要用ssc 这个checkpoint, 使用一下的
    ssc.sparkContext.setCheckpointDir("cp")
    val startupStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)
    //    startupStream.map(_.value()).foreachRDD { rdd =>
    //      println(rdd.collect().mkString("\n"))
    //    }

    val startupLogDstream: DStream[StartUpLog] = startupStream.map { log =>
      //      println(s"log = ${log}")
      val jsonStr = log.value()
      val startUpLog: StartUpLog = JSON.parseObject(jsonStr, classOf[StartUpLog])
      // 把日期进行补全
      val dateTimeStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(startUpLog.ts))
      //      println("dateTimeStr" + dateTimeStr)
      val dateTimeArray = dateTimeStr.split(" ")
      startUpLog.logDate = dateTimeArray(0)
      startUpLog.logHour = dateTimeArray(1).split(":")(0)
      startUpLog
    }
    // 如果上一个批次还没处理完，就需要使用这个
    startupLogDstream.cache()
    //    startupLogDstream.repartition(2)
    // 去重操作
    val filterDStream: DStream[StartUpLog] = startupLogDstream.transform { rdd =>
      // 每个时间间隔执行一次  driver
      println("去重前:" + rdd.count())
      val client = RedisUtil.getJedisClient
      val dateStr = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
      val key = "dau:" + dateStr
      // 日活清单
      val dauSet = client.smembers(key)
      client.close()
      val dauBd = ssc.sparkContext.broadcast(dauSet)
      val filterRDD = rdd.filter { startuplog =>
        // executor
        !dauBd.value.contains(startuplog.mid)
      }
      println("去重后:" + rdd.count())
      filterRDD
    }

    // 按照mid 进行分组，然后取第一个 自身也要过滤，第一次放入的时候， redis 中还没有
    val startupGroup = filterDStream.map(startupLog => (startupLog.mid, startupLog)).groupByKey()

    // 分组后取第一个, flatmap 之后只有一个集合，匹配后是个iterator
    val startupFilterDStream = startupGroup.flatMap {

      case (mid, str) => {
        str.take(1)
      }
    }

    // 将其放入到 redis
    startupFilterDStream.foreachRDD { rdd =>
      rdd.foreachPartition { log =>
        val client = RedisUtil.getJedisClient
        val list = log.toList
        for (startuplog <- list) {
          // 生成一个 key 放入
          val key = "dau:" + startuplog.logDate
          // redis set add 的方式  按照设备id 来确定是否重复
          client.sadd(key, startuplog.mid)
        }
        client.close()
      }
    }


    // 保存到hbase 中
    startupFilterDStream.foreachRDD { rdd =>
      val configuration = new Configuration()

      println(rdd.collect().mkString("\n"))
      rdd.saveToPhoenix(GmallConstants.ES_INDEX_DAU, Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"), configuration, Some("hadoop202,hadoop203,hadoop204:2181"))
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
