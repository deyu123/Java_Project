package com.deyu.jvm;

public class Test02 {

	
	public int add(int a ,int b)
	{
		int result = -1;
		result = a + b;

		return result;
	}
	
	
	public static void main(String[] args)
	{

		//双亲委派机制---JVM的安全性（沙箱机制）
	
		
		Object obj = new Object();
		System.out.println("********obj.getClass(): "+obj.getClass().getClassLoader());
		
		System.out.println();
		System.out.println();
		System.out.println();
		
		Test02 test02 = new Test02();
		
		
		System.out.println("********test02: "+test02.getClass().getClassLoader().getParent().getParent());
		System.out.println("********test02: "+test02.getClass().getClassLoader().getParent());
		System.out.println("********test02: "+test02.getClass().getClassLoader());
		
		new Thread().start();
		//JNI  元数据
	}

}
