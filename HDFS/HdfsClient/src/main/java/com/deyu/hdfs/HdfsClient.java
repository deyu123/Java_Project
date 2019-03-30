package com.deyu.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
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
//        boolean mkdirs = fs.mkdirs(new Path("/hadoop/zdy/aaa"));
        //log4j 需要调用者来制定
//        Logger logger = Logger.getLogger(HdfsClient.class);
//        logger.info("是否创建成功： " + mkdirs);
//        System.out.println("是否创建成功： " + mkdirs);
//        fs.close();

        //4. HDFS 的文件上传, 测试 配置的优先级
//        fs.copyFromLocalFile(new Path("e:/zdyjqf.txt"), new Path("/"));

        //5. HDFS 的文件删除, 包含文件夹，递归删除
//        boolean delete = fs.delete(new Path("/zdyjqf.txt"));
//        boolean delete = fs.delete(new Path("/aa"));
//        System.out.println("是否删除成功 ： " + delete);

        //6.文件的下载
//        fs.copyToLocalFile(new Path("/wcinput"), new Path("E:/"));

        //7. 修改文件名，rename,
        //8. 文件详情查看
//        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listFiles(new Path("/"), true);
//
//        while (locatedFileStatusRemoteIterator.hasNext()){
//            LocatedFileStatus status = locatedFileStatusRemoteIterator.next();
//            // 文件对象
//            System.out.println(status);
//            //路径
//            System.out.println(status.getPath().getName());
//            System.out.println(status.getLen());
//            System.out.println(status.getPermission());
//            System.out.println(status.getGroup());
//
//        }

        //9. 判断是文件， 还是文件夹
//        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
//        for (FileStatus fileStatus : fileStatuses) {
//            if(fileStatus.isFile()){
//                System.out.println(fileStatus + "是文件");
//            }else {
//                System.out.println(fileStatus + "是目录");
//            }
//
//        }

        //10. 文件的上传
//        FileInputStream fileInputStream = new FileInputStream(new File("e:/zdyjfq.txt"));
//        FSDataOutputStream fsDataOutputStream = fs.create(new Path("/banhua.txt"));
//        IOUtils.copyBytes(fileInputStream, fsDataOutputStream, configuration);
//        IOUtils.closeStream(fileInputStream);
//        IOUtils.closeStream(fsDataOutputStream);
//        fs.close();
//
        //11. 文件的下载
        FileOutputStream fos = new FileOutputStream(new File("E:/banhuajfq.txt"));
        FSDataInputStream fis = fs.open(new Path("/banhua.txt"));
        IOUtils.copyBytes(fis, fos, configuration);
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);

    }
}
