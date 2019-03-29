package com.deyu.thread;

import java.util.concurrent.locks.ReentrantReadWriteLock;

class MyQueue
{
	private Object obj;
	private ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
	
	public void writeObj(Object obj)
	{
		rwLock.writeLock().lock();
		try 
		{
			this.obj = obj;
			System.out.println(Thread.currentThread().getName()+"\t:"+obj);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			rwLock.writeLock().unlock();
		}
	}
	
	public void readObj()
	{
		rwLock.readLock().lock();
		try 
		{
			System.out.println(Thread.currentThread().getName()+"\t:"+obj);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			rwLock.readLock().unlock();
		}		
	}
}


/**
 *
 *读写锁
 *一个线程写，100个线程读取
 */
public class ThreadDemo05
{
	public static void main(String[] args)
	{
		MyQueue q = new MyQueue();
		
		new Thread(() -> {
			q.writeObj("class zdy");
		}, "writeThread").start();

		for (int i = 1; i <=100; i++) 
		{
			new Thread(() -> {
				q.readObj();
				// 给线程取名
			},"线程:" + i).start();
		}
		
	}

}
