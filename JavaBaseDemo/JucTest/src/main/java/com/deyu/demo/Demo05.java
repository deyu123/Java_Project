package com.deyu.demo;

import java.util.concurrent.locks.ReentrantReadWriteLock;

class MyQueue{

    private Object obj;
    private ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    public void readLock(){

        rwLock.readLock().lock();
        try {

            System.out.println(Thread.currentThread().getName() + "\t" + obj);

        } catch(Exception e){
            e.printStackTrace();
        }finally {
            rwLock.readLock().unlock();
        }


    }

    public  void  writeLock(Object obj){

        rwLock.writeLock().lock();

        try{
            this.obj = obj;
            System.out.println(Thread.currentThread().getName() + "write : " + obj);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            rwLock.writeLock().unlock();
        }


    }


}

public class Demo05 {

    public static void main(String[] args) {
        MyQueue myQueue = new MyQueue();

        new Thread( () ->{ myQueue.writeLock("zdy111"); }, "write thread").start();

        for(int i = 0 ; i < 100 ; i ++){
            new Thread( () -> { myQueue.readLock(); }, "read thread:" + i).start();
        }

    }

}
