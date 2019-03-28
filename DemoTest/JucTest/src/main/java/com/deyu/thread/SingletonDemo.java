package com.deyu.thread;

public class SingletonDemo {

    private static volatile SingletonDemo instance = null;

    private SingletonDemo() {
        System.out.println("***********" + Thread.currentThread().getName());
    }
//
//    public static SingletonDemo getInstance() {
//        if (null == instance) {
//            instance = new SingletonDemo();
//        }
//        return instance;
//    }


    //DCL(double check lock)

    // 双重加锁机制
    // 1. 私有化构造器
    // 2. 双重加锁返回
	public static SingletonDemo getInstance()
	{
		if(null == instance)
		{
			synchronized (SingletonDemo.class)
			{
				if(null == instance)
				{
					instance = new SingletonDemo();
				}
			}
		}
		return instance;
	}



    //懒汉式
    //单例模式如何写：
    //1. 私有化构造器
    //2. 创建静态实例
    //3. 提供静态方法，返回当前的类对象
    //好处： 节省了内存的空间， 延迟创建对象， 存在线程安全问题

//    public static SingletonDemo getInstance(){
//        if( null == instance){
//            instance = new SingletonDemo();
//        }
//
//        return  instance;
//
//    }

    //懒汉式
    // 不存在线程安全问题
    //1. 私有化构造器
    //2. 创建静态实例, ①静态代码块， ②  new 创建
    //3. 直接返回


//    private static SingletonDemo instance = new SingletonDemo();

//    static {
//        instance = new SingletonDemo();
//    }

//    public static SingletonDemo getInstance() {
//
//        return instance;
//    }




    public static void main(String[] args) {
        new Thread(() -> {
            SingletonDemo.getInstance();
        }, "A").start();
        new Thread(() -> {
            SingletonDemo.getInstance();
        }, "B").start();
        new Thread(() -> {
            SingletonDemo.getInstance();
        }, "C").start();
    }
}
