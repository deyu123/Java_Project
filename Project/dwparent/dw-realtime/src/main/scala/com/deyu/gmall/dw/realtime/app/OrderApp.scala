package com.deyu.gmall.dw.realtime.app

import com.alibaba.fastjson.JSON
import com.deyu.gmall.dw.bean.OrderInfo
import com.deyu.gmall.dw.common.constant.GmallConstants
import com.deyu.gmall.dw.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

object OrderApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("order_app")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER, ssc)

        inputDstream.map(_.value()).foreachRDD(rdd=>
          println(rdd.collect().mkString("\n"))
        )


    val orderInfoDstrearm: DStream[OrderInfo] = inputDstream.map {
      _.value()
    }.map { orderJson =>
      val orderInfo: OrderInfo = JSON.parseObject(orderJson, classOf[OrderInfo])
      //日期

      val createTimeArr: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date = createTimeArr(0)
      val timeArr: Array[String] = createTimeArr(1).split(":")
      orderInfo.create_hour = timeArr(0)
      // 收件人 电话 脱敏
      orderInfo.consignee_tel = "*******" + orderInfo.consignee_tel.splitAt(7)._2
      orderInfo
    }

    // 增加一个字段， 标识为是否是用户首次下单


    // 将其放入到 redis
    orderInfoDstrearm.foreachRDD { rdd =>
      rdd.foreachPartition { log =>
        val client = RedisUtil.getJedisClient
        val list = log.toList
        for (user <- list) {
          // 生成一个 key 放入
          val key = "user:" + user.id
          // value  放置用户信息的json 串
          client.sadd(key, user.toString)
        }
        client.close()
      }
    }

    orderInfoDstrearm.foreachRDD { rdd =>

      val configuration = new Configuration()
      println(rdd.collect().mkString("\n"))
      rdd.saveToPhoenix("GMALL_ORDER_INFO", Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"), configuration, Some("hadoop202,hadoop203,hadoop204:2181"))

    }

    ssc.start()
    ssc.awaitTermination()

  }

}