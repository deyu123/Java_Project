package com.deyu.thread;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * 
 * @author zhouyang
 * @version 创建时间：2017年11月23日  下午3:02:46
 */
public class ThreadDemo10 {

	public static void main(String[] args)
	{
		Map<String,String> map = new ConcurrentHashMap<String,String>();//new HashMap<String,String>();
		
		for (int i = 1; i <=25; i++) 
		{
			new Thread(() -> {
				map.put(Thread.currentThread().getName(),UUID.randomUUID().toString().substring(0, 9));
				System.out.println(map);
			},String.valueOf(i)).start();
		}		
	}

	private static void setNotSafe()
	{
		Set<String> set = new CopyOnWriteArraySet<String>();//new HashSet<String>();
		
		for (int i = 1; i <=25; i++) 
		{
			new Thread(() -> {
				set.add(UUID.randomUUID().toString().substring(0, 9));
				System.out.println(set);
			},String.valueOf(i)).start();
		}
	}

	private static void listNotSafe()
	{
		List<String> list = new CopyOnWriteArrayList<String>();//new ArrayList<String>();
		
		//多线程+高并发			---->		写时复制技术
		for (int i = 1; i <=35; i++) 
		{
			new Thread(() -> {
				list.add(UUID.randomUUID().toString().substring(0, 9));
				System.out.println(list);
			},String.valueOf(i)).start();
		}
	}

}