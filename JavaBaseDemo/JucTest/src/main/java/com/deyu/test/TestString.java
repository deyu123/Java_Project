package com.deyu.test;

public class TestString
{
	public static void main(String[] args)
	{
		String s1 = new String("abc");
		String s2 = "abc";
		String s3 = new String("abc");
		
		System.out.println(s1 == s2);
		System.out.println(s1 == s3);
		System.out.println(s2 == s3);
		System.out.println("====================");

		/**
		返回字符串对象的规范化表示形式。 
		一个初始为空的字符串池，它由类 String 私有地维护。 
		当调用 intern 方法时，如果池已经包含一个等于此 String 对象的字符串（用 equals(Object) 方法确定），
		则返回池中的字符串。否则，将此 String 对象添加到池中，并返回此 String 对象的引用。 
		它遵循以下规则：对于任意两个字符串 s 和 t，当且仅当 s.equals(t) 为 true 时，s.intern() == t.intern() 才为 true。 
		所有字面值字符串和字符串赋值常量表达式都使用 intern 方法进行操作。字符串字面值在 Java Language Specification 
		的 §3.10.5 定义。
		返回：一个字符串，内容与此字符串相同，但一定取自具有唯一字符串的池。		 */	
		
		System.out.println(s1 ==s1.intern());
		System.out.println(s2 ==s2.intern());
		System.out.println(s1.intern() ==s2.intern());
		
		System.out.println("====================");

		String s4 = "java";
		String s5 = "ja";
		String s6 = "va";

		System.out.println(s4 == "java");
		System.out.println(s4 == (s5+s6));
		System.out.println(s4 == "ja"+s6);

	}
}


