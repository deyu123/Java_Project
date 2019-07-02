package com.deyu.gmall.dw.realtime.app

import java.util

import com.alibaba.fastjson.JSON
import com.deyu.gmall.dw.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.deyu.gmall.dw.common.constant.GmallConstants
import com.deyu.gmall.dw.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.json4s.native.Serialization
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer
// java 集合转对象需要隐式转换类

import collection.JavaConversions._
object SalaApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sale_app")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER, ssc)
    val inputDetailDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL, ssc)
    val inputUserDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_USER, ssc)

    val orderInfoDstream: DStream[OrderInfo] = inputDstream.map { record =>
      val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
      //补充日期字段
      val datetimeArr: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date = datetimeArr(0)
      val timeArr: Array[String] = datetimeArr(1).split(":")
      orderInfo.create_hour = timeArr(0)
      //电话脱敏
      val telTuple: (String, String) = orderInfo.consignee_tel.splitAt(7)
      orderInfo.consignee_tel = "*******" + telTuple._2
      orderInfo
    }

    val orderDetailDStream: DStream[OrderDetail] = inputDetailDstream.map { record =>
      val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
      orderDetail
    }
    // DStream 来进行join 的时候，是根据key 来join， 所以需要转换结构
    val orderInfoWithKeyDstream: DStream[(String, OrderInfo)] = orderInfoDstream.map(orderinfo => (orderinfo.id, orderinfo))

    val orderDetailWithByKeyDStream: DStream[(String, OrderDetail)] = orderDetailDStream.map(orderdetail => (orderdetail.order_id, orderdetail))

    val fulljoinOrderDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoWithKeyDstream.fullOuterJoin(orderDetailWithByKeyDStream)

    println("aa")
    val fullSaleDetailDstream: DStream[SaleDetail] = fulljoinOrderDStream.mapPartitions { jsonItr =>
      val jedisClient: Jedis = RedisUtil.getJedisClient
      val saleDetailList: ListBuffer[SaleDetail] = ListBuffer[SaleDetail]()
      //添加隐式转换
      implicit val formats = org.json4s.DefaultFormats
      // TODO 1 左右两边都有数据， 关联上了，拼接成一个大对象
      for ((orderId, (orderInfoOption, orderDetailOption)) <- jsonItr) {
        if (orderInfoOption != None && orderDetailOption != None) {
          val orderInfo: OrderInfo = orderInfoOption.get
          val orderDetail: OrderDetail = orderDetailOption.get
          println("关联上了: " + orderInfo.id)
          val saleDetail = new SaleDetail(orderInfo, orderDetail)
          saleDetailList += saleDetail

          // TODO 1.1缓存主表，即使主表关联成功，也不能够保证其他的表不关联主表，所以主表必须缓存
          val orderInfoKey: String = "order_info:" + orderInfo.id
          // 需要添加隐式转换
          val orderInfoJson: String = Serialization.write(orderInfo)
          jedisClient.set(orderInfoKey, orderInfoJson)
          // TODO 1.2 不管主表中是否匹配上，都要去缓存查看是否有未匹配的从表
          val orderDetailKey: String = "order_detail:" + orderInfo.id
          val orderDetailJsonSet: util.Set[String] = jedisClient.smembers(orderDetailKey)
          for (orderDetailJson <- orderDetailJsonSet) {
            val orderDetail: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
            val saleDetail = new SaleDetail(orderInfo, orderDetail)
            saleDetailList += saleDetail
          }
        } else if (orderInfoOption != None && orderDetailOption == None) {
          //TODO 2 主表有 ， 从表没有
          val orderInfo: OrderInfo = orderInfoOption.get
          println("主表有+++，从表没有---:" + orderInfo.id)
          val orderDetailKey: String = "order_detail:" + orderInfo.id
          // TODO 2.1 主表有，redis 中也有就会匹配上
          val orderDetailJsonSet: util.Set[String] = jedisClient.smembers(orderDetailKey)
          //
          if (orderDetailJsonSet != null && orderDetailJsonSet.size() != 0) {
            // 遍历， 匹配
            for (orderDetailJson <- orderDetailJsonSet) {
              val orderDetail: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
              val saleDetail = new SaleDetail(orderInfo, orderDetail)
              saleDetailList += saleDetail
            }
            jedisClient.del(orderDetailKey)
          } else {
            //TODO 2.2 主表有， 从表也没有
            println("主表" + orderInfo.id + "来早了")
          }
          //TODO 2.3 主表有， 从表也没有， 也要放入 redis 中
          val orderInfokey = "order_info:" + orderInfo.id
          val orderInfoJson: String = Serialization.write(orderInfo)
          jedisClient.set(orderInfokey, orderInfoJson)
        } else {
          //TODO 3 主表没有，从表中有， 查询主表，如果没有查到保存自己
          val orderDetail: OrderDetail = orderDetailOption.get
          //TODO 3.1 查询主表, 中有
          val orderInfokey = "order_info:" + orderDetail.order_id
          val orderInfojson: String = jedisClient.get(orderInfokey)
          if (orderInfojson != null && orderInfojson.length > 0) { //从表来晚
            val orderInfo: OrderInfo = JSON.parseObject(orderInfojson, classOf[OrderInfo])
            val saleDetail = new SaleDetail(orderInfo, orderDetail)
            saleDetailList += saleDetail
          } else {
            //TODO 3.2 查询主表, 中没有，将自己(从表)写入缓存
            val orderDetailKey = "order_detail:" + orderDetail.order_id
            val orderDetailJson: String = Serialization.write(orderDetail)
            jedisClient.sadd(orderDetailKey, orderDetailJson)
          }

        }
      }

      jedisClient.close()
      saleDetailList.toIterator
    }

//    fulljoinOrderDStream.foreachRDD{rdd=>
//         println(rdd.collect().mkString("\n"))
//       }

    // 把userInfo 保存到缓存中

    inputUserDstream.map{record=>
      val userInfo: UserInfo = JSON.parseObject(record.value(), classOf[UserInfo])
      userInfo
    }.foreachRDD{rdd:RDD[UserInfo]=>
      val userList: List[UserInfo] = rdd.collect().toList
      val jedis: Jedis = RedisUtil.getJedisClient
      implicit val formats=org.json4s.DefaultFormats
      for (userInfo <- userList ) {   //  string  set list hash zset
        //设计user_info  redis  type  hash      key   user_info  , field   user_id  ,value user_info_json
        val userkey="user_info"
        val userJson: String = Serialization.write(userInfo)
        jedis.hset(userkey,userInfo.id,userJson)
      }
      jedis.close()
    }

    ssc.start()
    ssc.awaitTermination()

  }
}
