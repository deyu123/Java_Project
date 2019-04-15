package com.deyu.hadoop.zookeeper;

import org.apache.zookeeper.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class ZookeeperTest {
    String connectString = "hadoop202:2181,hadoop203:2181,hadoop204:2181";
    int sessionTimeout = 2000;
    ZooKeeper zooKeeper = null;
    @Before
    public void Before() throws IOException {
        zooKeeper = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
                    @Override
                    public void process(WatchedEvent watchedEvent) {
                        System.out.println("回调函数");
                    }
                });

    }

    @Test
    public void testzoo() throws KeeperException, InterruptedException {
        String s = zooKeeper.create("/testCreate", "111".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(s);
    }

    @Test
    public void create(){
    }

    @Test
    public void testWatch(){
        
    }



    public void getChildren(String path) throws KeeperException, InterruptedException {
        try {
            zooKeeper.getChildren(path, new Watcher() {
                @Override
                public void process(WatchedEvent watchedEvent) {

                    getChildren(path);

                }
            });
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }
}
