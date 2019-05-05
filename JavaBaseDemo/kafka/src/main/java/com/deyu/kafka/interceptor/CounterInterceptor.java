package com.deyu.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class CounterInterceptor implements ProducerInterceptor<String,String > {

    private Long successCount = 0L;
    private Long errorCount = 0L;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        // 如果修改的话，返回一个新的 record
        // 如果不修改，也要返回原来的record
        return producerRecord;
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if(e == null){
            successCount ++;
        }else {
            errorCount ++;
        }
    }

    @Override
    public void close() {
        System.out.println("success : " + successCount);
        System.out.println("error : " + errorCount);
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
