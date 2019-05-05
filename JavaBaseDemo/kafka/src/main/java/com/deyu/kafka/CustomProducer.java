package com.deyu.kafka;

import org.apache.kafka.clients.producer.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class CustomProducer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop202:9092");//kafka集群，broker-list

        props.put("acks", "all");
        props.put("retries", 1);//重试次数
        props.put("batch.size", 16384);//批次大小
        props.put("linger.ms", 1);//等待时间
        props.put("buffer.memory", 33554432);//RecordAccumulator缓冲区大小

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 构建拦截器链
        List<String> interceptors = new ArrayList<String>();
        interceptors.add("com.deyu.kafka.interceptor.TimeInterceptor");
        interceptors.add("com.deyu.kafka.interceptor.CounterInterceptor");

        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);


        // 创建一个kafkaProducer 对象
        Producer<String, String> producer = new KafkaProducer<>(props);
        //1. 不带回调函数的API
//        for (int i = 0; i < 100; i++) {
//            // 调用 send 方法
//            producer.send(new ProducerRecord<String, String>("first", i + "", "message-" + i), new Callback() {
//                // 判断是否重试
//                @Override
//                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
//
//                    if(e == null){
//                        System.out.println(recordMetadata.topic() + "--" + recordMetadata.partition() + "--" + recordMetadata.offset() + "-- success");
//                    }else{
//                        e.printStackTrace();
//                    }
//                }
//            });
//        }



        //2. 带回调函数的API
//        for (int i = 0; i < 100; i++) {
//            producer.send(new ProducerRecord<String, String>("first", Integer.toString(i), Integer.toString(i)), new Callback() {
//
//                //回调函数，该方法会在Producer收到ack时调用，为异步调用
//                @Override
//                public void onCompletion(RecordMetadata metadata, Exception exception) {
//                    if (exception == null) {
//                        System.out.println("success->" + metadata.offset());
//                    } else {
//                        exception.printStackTrace();
//                    }
//                }
//            });
//        }

        //3. 同步发送的意思就是，一条消息发送之后，会阻塞当前线程，直至返回ack。

//        for (int i = 0; i < 100; i++) {
//            producer.send(new ProducerRecord<String, String>("first", Integer.toString(i), Integer.toString(i))).get();
//        }

        // 3 发送消息
        for (int i = 0; i < 10; i++) {

            ProducerRecord<String, String> record = new ProducerRecord<>("first", "message--" + i);
            producer.send(record);
        }

        // 4 一定要关闭producer，这样才会调用interceptor的close方法
        producer.close();
    }

}
