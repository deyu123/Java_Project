package com.deyu.test;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;


/**
 * 
 * @author zhouyang
 *
 */
public class Client {

	private static final Logger logger = Logger.getLogger(Client.class);
	
	
	private Lock lock = new ReentrantLock();
	

	
	public void T()
	{
		lock.lock();
		try 
		{
			logger.info("*****************333");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			lock.unlock();
		}
	}
	
	public static void main(String[] args) 
	{
		new Client().T();
	}

}
