package com.deyu.flume.myinterceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;

public class myinterceptor implements Interceptor {

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        if('1'<=event.getBody()[0]&&event.getBody()[0]<='9'){
            event.getHeaders().put("topic","number");
        }else if('a'<=event.getBody()[0]&&event.getBody()[0]<='z'){
            event.getHeaders().put("topic","letter");
        }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        return null;
    }

    @Override
    public void close() {

    }

    public static class CustomBuilder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new myinterceptor();
        }

        @Override
        public void configure(Context context) {
        }
    }
}
