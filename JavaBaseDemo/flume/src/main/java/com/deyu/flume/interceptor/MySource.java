package com.deyu.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

import java.util.HashMap;

public class MySource extends AbstractSource implements Configurable, PollableSource {

    private String prefix ;
    private Long delay;


    /**
     * 处理Event 的方法， 在该方法中， 把 Event 交给 channel
     * @return
     * @throws EventDeliveryException
     */
    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;
        // 1.模拟处理五条数据
        try {
            // 2.新建一个Event
            SimpleEvent event = new SimpleEvent();
            // 创建一个头信息
            HashMap<String, String> header = new HashMap<>();
            //循环封装
            for (int i = 0; i < 5; i++) {
                event.setBody((prefix + i).getBytes());
                event.setHeaders(header);
                // 3.得到一个channelProcess
                ChannelProcessor channelProcessor = getChannelProcessor();
                channelProcessor.processEvent(event);
                // 4. 得到状态 , 如何得知， try catch
                // 5. 自定义的等待时间
                Thread.sleep(delay);
            }

            status = Status.READY;
        } catch (Exception e) {
            status = Status.BACKOFF;
            e.printStackTrace();
        }
        return status;
    }

    /**
     * 重试又失败了，重试间隔时间增加值
     * @return
     */
    @Override
    public long getBackOffSleepIncrement() {
        return 1000;
    }

    /**
     *
     * 设置重试提交时间
     * @return
     */
    @Override
    public long getMaxBackOffSleepInterval() {
        return 1000;
    }

    /**
     * 读取配置
     * @param context
     */
    @Override
    public void configure(Context context) {
        prefix = context.getString("prefix");
        delay = context.getLong("delay");

    }

}
