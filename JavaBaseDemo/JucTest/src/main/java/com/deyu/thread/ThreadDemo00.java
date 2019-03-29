package com.deyu.thread;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


class Ticket
{
	private int number = 30;//Field
	private Lock lock = new ReentrantLock();//List list = new ArrayList();
	
	public void sale()
	{
		lock.lock();
		try 
		{
			if(number > 0)
			{
				System.out.println(Thread.currentThread().getName()+"\t卖出第："+(number--)+"\t还剩下："+number);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			lock.unlock();
		}
		
	}
	//code Templates
}

/**
 * int number = 30;  Field
 * sale()			 Method
 * 
 * 
 *
 *	题目：3个售票员			卖出			30张票	（工程代码）
 *
 *1	线程		操作		资源类
 *2	高内聚	低耦合
 *结论：资源类自身以高内聚的方式，自身携带同步方法，对外暴露给多线程调用
 *
 *Thread(Runnable target, String name) 	Allocates a new Thread object.
 */
public class ThreadDemo00 
{
	public static void main(String[] args)
	{
		Ticket ticket = new Ticket();
		
		new Thread( () -> {for (int i = 1; i <=40; i++) ticket.sale();} , "1").start();
		new Thread( () -> {for (int i = 1; i <=40; i++) ticket.sale();} , "2").start();
		new Thread( () -> {for (int i = 1; i <=40; i++) ticket.sale();} , "3").start();


		
		/*new Thread(new Runnable() {
			@Override
			public void run()
			{
				for (int i = 1; i <=40; i++) 
				{
					ticket.sale();
				}
			}
		},"AA").start();
		new Thread(new Runnable() {
			@Override
			public void run()
			{
				for (int i = 1; i <=40; i++) 
				{
					ticket.sale();
				}
			}
		}, "BB").start();
		new Thread(new Runnable() {
			@Override
			public void run()
			{
				for (int i = 1; i <=40; i++) 
				{
					ticket.sale();
				}
			}
		}, "CC").start()*/
		
	}
}
