package com.deyu.demo;


import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class Ticket {

    private int ticketNum = 30; //Field
    Lock rt = new ReentrantLock();

    public void sale() {
        rt.lock();

        try {
            if (ticketNum > 0) {
                System.out.println(Thread.currentThread().getName() + ", 卖了 : " + (ticketNum -- ) + "剩余: " + ticketNum);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            rt.unlock();

        }

    }

}


public class Demo01 {

    public static void main(String[] args) {
        Ticket ticket = new Ticket();

        new Thread(() -> {for(int i = 0 ; i < 10 ; i ++) ticket.sale();},"AA").start();
        new Thread(() -> {for(int i = 0 ; i < 10 ; i ++) ticket.sale();},"BB").start();
        new Thread(() -> {for(int i = 0 ; i < 10 ; i ++) ticket.sale();},"CC").start();

//        new Thread(new Runnable() {
//            @Override
//            public void run() {
//                for(int i = 0 ; i< 10 ; i ++){
//                    ticket.sale();
//                }
//            }
//        }, "AA").start();
//
//        new Thread(new Runnable() {
//            @Override
//            public void run() {
//                for(int i = 0 ; i< 10 ; i ++){
//                    ticket.sale();
//                }
//            }
//        }, "BB").start();
//
//        new Thread(new Runnable() {
//            @Override
//            public void run() {
//                for(int i = 0 ; i< 10 ; i ++){
//                    ticket.sale();
//                }
//            }
//        }, "CC").start();
    }

}

