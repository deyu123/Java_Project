package com.deyu.test;

import com.deyu.sparkmall.common.util.MyKafkaUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.StreamingContext;
import redis.clients.jedis.Jedis;

public class TestJedis {
    public static void main(String[] args) {
        Jedis jedis = new Jedis("hadoop202", 6379);
        System.out.println("connectiong is : " + jedis.ping());

    }
}
