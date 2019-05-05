package com.deyu.kafka;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Properties;

public class CustomConsumer {
    private static HashMap<TopicPartition, Long> currentOffset  = new HashMap<>();
    public static void main(String[] args) {
        //自动提交offset的功能。
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop202:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // 订阅哪个主题
//        consumer.subscribe(Arrays.asList("first"));
        consumer.subscribe(Arrays.asList("first"), new ConsumerRebalanceListener() {
            // 回收之前调用
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
//                System.out.println("被回收的partition");
//                for (TopicPartition topicPartition : collection) {
//                    System.out.println("Revoked partiiton--" + topicPartition);
//                }
                commitOffset(currentOffset);
            }

            // 分配之后调用
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
//                System.out.println("被分配的partition");
//                for (TopicPartition topicPartition : collection) {
////                    System.out.println("Assigned Partition--" + topicPartition);
//                    Long offset = getOffset(topicPartition);
//
//                }
                currentOffset.clear();
                for (TopicPartition partition : collection) {

                    consumer.seek(partition, getOffset(partition));
                }
            }
        });

        // 自动提交offset
//        while (true) {
//            // 消费的时候，没有数据， 返回为空，给他一个timeout, 让他等一会在返回。
//            ConsumerRecords<String, String> records = consumer.poll(100);
//
//            for (ConsumerRecord<String, String> record : records)
//                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
//        }
        // 手动同步提交offset
//        while (true) {
//            ConsumerRecords<String, String> records = consumer.poll(100);//消费者拉取数据
//            for (ConsumerRecord<String, String> record : records) {
//                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
//            }
//            consumer.commitSync();//同步提交，当前线程会阻塞知道offset提交成功
//        }

        // 自定义offset

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);//消费者拉取数据
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                currentOffset.put(new TopicPartition(record.topic(), record.partition()), record.offset());
                commitOffset(currentOffset);
            }
        }

    }
    //获取某分区最新的offset
    private static long commitOffset(HashMap<TopicPartition, Long> currentOffset) {
        return 0;
    }

    // 根据topic 获取offset
    private static Long getOffset(TopicPartition topicPartition) {
        return null;
    }




}
