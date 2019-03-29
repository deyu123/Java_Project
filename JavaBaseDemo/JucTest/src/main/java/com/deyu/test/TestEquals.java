package com.deyu.test;

import java.util.HashSet;
import java.util.Set;

import com.deyu.entities.Person;

public class TestEquals
{
	public static void main(String[] args)
	{
		String s1 = new String("abc");
		String s2 = new String("abc");
		System.out.println(s1 == s2);
		System.out.println(s1.equals(s2));
		Set<String> set01 = new HashSet<String>();
		set01.add(s1);
		set01.add(s2);
		System.out.println(s1.hashCode()+"\t"+s2.hashCode());
		System.out.println(set01.size());

		System.out.println("================================");
		
		Person p1 = new Person("wanghai");
		Person p2 = new Person("wanghai");
		System.out.println(p1 == p2);
		System.out.println(p1.equals(p2));
		Set<Person> set02 = new HashSet<Person>();
		set02.add(p1);
		set02.add(p2);
		System.out.println(p1.hashCode()+"\t"+p2.hashCode());
		System.out.println(set02.size());

	}
}
