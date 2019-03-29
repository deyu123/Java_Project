package com.deyu.demo;

import java.util.concurrent.CountDownLatch;

public class Demo07 {

    public static void main(String[] args) throws InterruptedException {

        CountDownLatch cd = new CountDownLatch(6);

        for(int i = 0 ;i < 6; i ++){

            new Thread(() -> {
                System.out.println(Thread.currentThread().getName() + "同学离开");
                cd.countDown();
                }, "同学： " + i).start();

        }
        cd.await();
        System.out.println(Thread.currentThread().getName() + "班长关门");

    }
}
