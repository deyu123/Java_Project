package com.deyu.thread;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

class MyThread implements Callable<Integer> 
{

	@Override
	public Integer call() throws Exception
	{
		System.out.println("*****call()*************");
		Thread.sleep(4000);
		return 200;
	}
}

/**
 * Callable接口获得多线程
 * Thread(Runnable target, String name) Allocates a new Thread object.
 */
public class ThreadDemo01
{
	public static void main(String[] args) throws InterruptedException, ExecutionException
	{
		FutureTask<Integer> ft = new FutureTask<Integer>(new MyThread());
		new Thread(ft, "AA").start();
		Integer resultA = ft.get();
		System.out.println("*******main****:resultA"+resultA);

		new Thread(ft, "BB").start();

		System.out.println(Thread.currentThread().getName()+"#######");

		Integer resultB = ft.get();
		System.out.println("*******main****:resultB"+resultB);
		
//		FutureTask<Integer> ft = new FutureTask<Integer>(() -> {
//			System.out.println("---------200");
//			Thread.sleep(4000);
//			return 200;
//		});
//		new Thread(ft, "A").start();
//
//		ft.get();
//
//		System.out.println(Thread.currentThread().getName()+" 111111111111");
	}
}


/**
 在主线程中需要执行比较耗时的操作时，但又不想阻塞主线程时，可以把这些作业交给Future对象在后台完成，当主线程将来需要时，
 就可以通过Future对象获得后台作业的计算结果或者执行状态。

一般FutureTask多用于耗时的计算，主线程可以在完成自己的任务后，再去获取结果。

仅在计算完成时才能检索结果；如果计算尚未完成，则阻塞 get 方法。一旦计算完成，就不能再重新开始或取消计算。
get方法而获取结果只有在计算完成时获取，否则会一直阻塞直到任务转入完成状态，然后会返回结果或者抛出异常。 
*/