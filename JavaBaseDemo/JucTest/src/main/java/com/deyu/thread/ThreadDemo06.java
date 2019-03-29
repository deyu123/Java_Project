package com.deyu.thread;

import org.junit.Test;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;


/**
 * 线程池，近似的理解java里面第4种获得多线程的方式
 * 3s
 * Collections	Arrays	Executors
 */
public class ThreadDemo06 {
    public static void main(String[] args) {
        ScheduledExecutorService service = Executors.newScheduledThreadPool(5);
        ScheduledFuture<Integer> result = null;

        try {
            for (int i = 1; i <= 10; i++) {
                result = service.schedule(() -> {
                    System.out.print(Thread.currentThread().getName());
                    return new Random().nextInt(20);
                }, 1, TimeUnit.MINUTES);
                System.out.println("******result: " + result.get());
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            service.shutdown();
        }
    }

    @Test
    public void threadPoolTest() {
        ExecutorService service = Executors.newFixedThreadPool(5);//new ThreadPoolExecutor
        //ExecutorService service = Executors.newSingleThreadExecutor();//new ThreadPoolExecutor
        //ExecutorService service = Executors.newCachedThreadPool();//new ThreadPoolExecutor
        Future<Integer> result = null;


        try {
            for (int i = 1; i <= 10; i++) //10个客户来银行办理业务，只有5个办理窗口
            {
                result = service.submit(() -> {
                    System.out.print(Thread.currentThread().getName());
                    return new Random().nextInt(20);
                });
                System.out.println("******result: " + result.get());//回执单
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            service.shutdown();
        }
    }


}
