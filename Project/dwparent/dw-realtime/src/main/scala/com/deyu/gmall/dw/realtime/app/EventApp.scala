package com.deyu.gmall.dw.realtime.app


import com.alibaba.fastjson.JSON
import com.deyu.gmall.dw.bean.{CouponAlertInfo, EventInfo}
import com.deyu.gmall.dw.common.constant.GmallConstants
import com.deyu.gmall.dw.realtime.util.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import util.control.Breaks._
import java.util

object EventApp {
  def main(args: Array[String]) = {
    val sparkConf: SparkConf = new SparkConf().setAppName("event_app").setMaster("local[*]")

    val ssc = new StreamingContext(new SparkContext(sparkConf),Seconds(5))

    val inputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT,ssc)

    val eventInfoDstream: DStream[EventInfo] = inputDStream.map { record =>
      val eventInfo: EventInfo = JSON.parseObject(record.value(), classOf[EventInfo])
      eventInfo
    }
    eventInfoDstream.cache()
    // 日志预警
    //需求：同一设备，5分钟(测试用30秒)内连续三次用不同账号登录并领取优惠劵，并且在登录到领劵过程中没有浏览商品。产生一条预警日志。
    //同一设备每分钟 只记录一次预警
    // 从事件日志中读取数据


//    eventInfoDstream.foreachRDD(rdd => rdd.foreach(println))
    // 先开窗口
    val eventInfoDstreamWindow = eventInfoDstream.window(Seconds(30), Seconds(5))
    // 按照设置id 分组
    val eventInfoDstreamGroup = eventInfoDstreamWindow.map(x =>(x.mid,x)).groupByKey()

    // 筛选打标记
    // 连续三个 sessionid ,都领取优惠卷且没有 点击商品

    val couponAlertWithIdDstream: DStream[(Boolean, CouponAlertInfo)] = eventInfoDstreamGroup.map {
      case (mid, eventInfo) =>
        var field: Boolean = true
        val couponUidSet: util.HashSet[String] = new util.HashSet()
        val couponItemIdSet: util.HashSet[String] = new util.HashSet()
        val events: util.ArrayList[String] = new util.ArrayList()
        breakable {
          for (event <- eventInfo) {
            events.add(event.evid)
            // 领取优惠卷 保存在set 中
            if (event.evid == "coupon") {
              couponUidSet.add(event.uid)
              couponItemIdSet.add(event.itemid)
            } else if (event.evid == "clickItem") {
              field = false
              break()
            }
          }
        }
        println("size:" + couponUidSet.size())
        println("field:" + field)
        (field && couponUidSet.size() >= 3, CouponAlertInfo(mid, couponUidSet, couponItemIdSet, events, System.currentTimeMillis()))
    }

    // 过滤
    val couponAlertInfoDstream: DStream[CouponAlertInfo] = couponAlertWithIdDstream.filter {
      case (flag, couponAlertInfo: CouponAlertInfo) => flag
    }.map(_._2)
    //幂等处理, 处理一直预警
    val couponAlertWithDStream: DStream[(String, CouponAlertInfo)] = couponAlertInfoDstream.map(couponAlertInfo => (couponAlertInfo.mid+"_" + couponAlertInfo.ts/1000L/60L,couponAlertInfo))

    // 保存在 ES 中
    couponAlertWithDStream.foreachRDD{rdd =>
      rdd.foreachPartition{ alertItem =>
        MyEsUtil.insertEsBulk(GmallConstants.ES_INDEX_COUPON_ALERT, alertItem.toList)
      }
    }

    ssc.start()
    ssc.awaitTermination()

  }

}
