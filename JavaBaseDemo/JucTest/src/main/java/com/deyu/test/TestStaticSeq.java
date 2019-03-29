package com.deyu.test;


class Father
{
	public Father()
	{
		System.out.println("111111");
	}
	{
		System.out.println("222222");
	}
	static{
		System.out.println("333333");
	}
}
class Son extends Father
{
	public Son()
	{
		super();
		System.out.println("444444");
	}
	{
		System.out.println("555555");
	}
	static{
		System.out.println("666666");
	}	
}
public class TestStaticSeq
{
	public static void main(String[] args)
	{
		new Son();
		System.out.println("======================");

		System.out.println("======================");

	}
}
