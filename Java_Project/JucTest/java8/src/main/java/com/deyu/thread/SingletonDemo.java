package com.deyu.thread;

public class SingletonDemo
{

	private static volatile SingletonDemo instance = null;
	
	private SingletonDemo() 
	{
		System.out.println("***********"+Thread.currentThread().getName());
	}
	
	public static SingletonDemo getInstance()
	{
		if(null == instance)
		{
			instance = new SingletonDemo();
		}
		return instance;
	}
	
	//DCL(double check lock)

	// 双重
//	public static SingletonDemo getInstance()
//	{
//		if(null == instance)
//		{
//			synchronized (SingletonDemo.class)
//			{
//				if(null == instance)
//				{
//					instance = new SingletonDemo();
//				}
//			}
//		}
//		return instance;
//	}

	
	public static void main(String[] args)
	{
		new Thread(() -> {SingletonDemo.getInstance();}, "A").start();
		new Thread(() -> {SingletonDemo.getInstance();}, "B").start();
		new Thread(() -> {SingletonDemo.getInstance();}, "C").start();
	}
}
