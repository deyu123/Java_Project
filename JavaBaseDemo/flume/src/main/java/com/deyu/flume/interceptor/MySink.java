package com.deyu.flume.interceptor;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

import java.io.FileOutputStream;
import java.io.IOException;

public class MySink extends AbstractSink implements Configurable {
    private String file;
    /**
     * 处理数据， 从channel中拉取数据
     * @return
     * @throws EventDeliveryException
     */
    @Override
    public Status process() throws EventDeliveryException {
        // 1. status
        Status status = null;
        // 2. start transaction
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        transaction.begin();


        try {
            // 成功处理 event
            Event event ;
            // 处理不成功， 会一直task
            while ((event = channel.take()) == null ){
                Thread.sleep(100);
            }

            // 3. 写出到文件
            FileOutputStream fos = new FileOutputStream(file, true);
            try {
                fos.write(event.getBody());
                fos.write("\n".getBytes());
            } catch (IOException e) {
                throw e;
            } finally {
                fos.close();
            }

            status = Status.READY;
            transaction.commit();
        } catch (Exception e) {
            // 失败回滚
            status = Status.BACKOFF;
            transaction.rollback();
            e.printStackTrace();
        } finally {
            transaction.close();
        }
        return  status;
    }

    /**
     * 获取配置
     * @param context
     */
    @Override
    public void configure(Context context) {
        file = context.getString("file");
    }
}
