package com.deyu.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


public class HdfsClient {

    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        //1. 获取文件系统
        Configuration configuration = new Configuration();
        //2.这种需要指定 用户名 -DHADOOP_USER_NAME=zdy
//        configuration.set("fs.defaultFS", "hdfs://hadoop102:9000");
//        FileSystem fs = FileSystem.get(configuration);

        //3. 直接填写用户名的API
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "zdy");
        boolean mkdirs = fs.mkdirs(new Path("/hadoop/zdy/aaa"));
        //log4j 需要调用者来制定
//        Logger logger = Logger.getLogger(HdfsClient.class);
//        logger.info("是否创建成功： " + mkdirs);
        System.out.println("是否创建成功： " + mkdirs);
        fs.close();
    }
}
