package com.deyu.demo;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

class MyThread implements Callable<Integer> {

    @Override
    public Integer call() throws Exception {

        System.out.println(Thread.currentThread().getName() + "****call ****");
        Thread.sleep(4000);
        return 2019;
    }
}

public class Demo00 {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        FutureTask<Integer> ft = new FutureTask<>(new MyThread());
        new Thread(ft,"AA" ).start();

        System.out.println("main runing");
        // 会阻塞线程的运行
        Integer ftResult = ft.get();

        System.out.println(Thread.currentThread().getName() + ftResult);
    }
}
