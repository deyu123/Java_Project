package com.deyu.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HBaseTest {

    // 创建一个线程安全的connection
    private static Connection connection = null;
    public synchronized static Connection getConnection() throws IOException {
        if (connection == null){
            final Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "hadoop202,hadoop203,hadoop204");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            connection = ConnectionFactory.createConnection(conf);
        }
        return connection;
    }

    public static void dropTable(String tableName){

    }

}
