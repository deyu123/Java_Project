package com.deyu.demo;

import java.util.concurrent.Semaphore;

public class Demo08 {

    public static void main(String[] args) {
        // 模拟三辆车
        Semaphore semaphore = new Semaphore(3);
        // 模拟6 辆车
        for (int i = 0; i < 6; i++) {

            new Thread(() -> {

                try {
                    semaphore.acquire();

                    System.out.println(Thread.currentThread().getName() + "停车");
                    Thread.sleep(3000);
                    System.out.println(Thread.currentThread().getName() + "离开");

                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    semaphore.release();
                }

            }, i + "号车").start();

        }
    }
}
