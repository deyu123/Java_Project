package com.deyu.test;

@FunctionalInterface
interface Foo
{
//	public void say886();
//	public void sayHello();
	
	public int add(int x, int y);
	
	default int div(int x, int y)
	{
		return x/y;
	}
	public static int div2(int x, int y)
	{
		return x/y;
	}
}


/**
 * 1	(形参列表) -> {方法体的实现}
 * 2	@FunctionalInterface
 * 3	default方法

 * @author lenovo
 *
 */
public class LambdaDemo
{

	public static void main(String[] args)
	{
//		Foo test = new Foo() {
//			@Override
//			public void say886()
//			{
//				System.out.println("****886 2017");
//			}
//
//			@Override
//			public void sayHello()
//			{
//				
//			}
//		};
//		test.say886();
		
//		test = () -> {System.out.println("****886 2017 lambda");};
//		test.say886();
		
		Foo test = (x,y) -> {return x+y;};
		int result = test.add(3, 12);
		System.out.println("**********result: "+result);
		
		System.out.println(test.div(10, 2)); 
		
		System.out.println(Foo.div2(10, 5));
	}

}
