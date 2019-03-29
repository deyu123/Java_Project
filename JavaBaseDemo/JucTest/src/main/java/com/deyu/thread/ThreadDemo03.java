package com.deyu.thread;

import java.util.concurrent.TimeUnit;


/**
 * 
一个对象里面如果有多个synchronized方法，某一个时刻内，只要一个线程去调用其中的一个synchronized方法了，
其它的线程都只能等待，换句话说，某一个时刻内，只能有唯一一个线程去访问这些synchronized方法

锁的是当前对象this，被锁定后，其它的线程都不能进入到当前对象的其它的synchronized方法

加个普通方法后发现和同步锁无关

锁的是当前的Class
 *
 */
class Phone
{
	public static synchronized void getIOS() throws Exception
	{
		TimeUnit.SECONDS.sleep(4);
		System.out.println("*****getIOS");
	}
	
	public synchronized void getAndroid() throws Exception
	{
		System.out.println("*****getAndroid");
	}
	
	public void getHello() throws Exception
	{
		System.out.println("*****getHello");
	}
}

/**
 * 
 * @author zhouyang
 *多线程的锁（8锁）
 *
 *1	标准访问，请问先打印苹果还是Android
 *2	暂停4秒，请问先打印苹果还是Android
 *3 新增hello方法，请问先打印苹果还是hello
 *4 两部手机，请问先打印苹果还是Android
 *5 两个静态同步方法，同一部手机，请问先打印苹果还是Android
 *6 两个静态同步方法，有2部手机，请问先打印苹果还是Android
 *7 一个普通同步方法，一个静态同步方法，同一部手机，请问先打印苹果还是Android
 *8 一个普通同步方法，一个静态同步方法，有2部手机，请问先打印苹果还是Android
 */
public class ThreadDemo03
{
	public static void main(String[] args) throws InterruptedException
	{
		Phone phone = new Phone();
		Phone phone2 = new Phone();
		
		new Thread(() -> {
			try {
				phone.getIOS();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}, "A").start();
		
		Thread.sleep(100);

		new Thread(() -> {
			try 
			{
				//phone.getAndroid();
				//phone.getHello();
				phone2.getAndroid();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}, "B").start();
		
		
	}
}
