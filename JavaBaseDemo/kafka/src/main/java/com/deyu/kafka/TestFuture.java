package com.deyu.kafka;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestFuture {
    public static void main(String[] args) {
        ExecutorService service = Executors.newCachedThreadPool();
        service.submit(new Runnable() {
            @Override
            public void run() {
                System.out.println("11");
            }
        });

    }
}
