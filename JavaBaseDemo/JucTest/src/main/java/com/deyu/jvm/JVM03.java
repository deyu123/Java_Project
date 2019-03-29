package com.deyu.jvm;

import java.util.ArrayList;
import java.util.List;

public class JVM03 
{
	byte[] byteArray = new byte[1*1024*1024]; //1MB

	public static void main(String[] args)
	{
		List<JVM03> list = new ArrayList<JVM03>();
		
		try 
		{
			while(true)
			{
				list.add(new JVM03());
			}
		}catch (Throwable e) {
			System.out.println("*****************************************haha");
			e.printStackTrace();
		}
	}

}
