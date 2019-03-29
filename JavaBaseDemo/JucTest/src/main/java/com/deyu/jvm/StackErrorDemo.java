package com.deyu.jvm;

public class StackErrorDemo {

	public static void test01(int x)
	{

	}
	
	
	public static void main(String[] args)
	{
		Object obj = new Object();
	}

}
//Exception in thread "main" java.lang.StackOverflowError	www.StackOverflow.com