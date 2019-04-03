package com.deyu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;


public class hdfsAPITest {
    FileSystem fs;
    @Before
    public void  before() throws IOException, InterruptedException {
        fs = FileSystem.get(URI.create("hdfs://hadoop202:9000"), new Configuration(), "deyu");
    }

    @Test
    public void listFiles() throws IOException {
        RemoteIterator<LocatedFileStatus> remoteIterator = fs.listFiles(new Path("/"), true);
        while (remoteIterator.hasNext()){
            LocatedFileStatus fileStatus = remoteIterator.next();
            System.out.println(fileStatus.getPath());
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {

                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.print(host + " ") ;
                }
                System.out.println();

            }
            System.out.println("============================================");
        }

    }


    @Test
    public void put() throws IOException {
        fs.copyFromLocalFile(new Path("F:\\Develop\\hadoop-2.7.2\\README.txt"), new Path("/"));
    }


    @Test
    public void open() throws IOException {
        FSDataInputStream open = fs.open(new Path("/a.txt"));
        byte[] bytes = new byte[1024];
        int len;
        while ((len = open.read(bytes)) > 0){
            String s = new String(bytes, 0, len);
            System.out.println(s);
        }
        IOUtils.closeStream(open);

    }

    @Test
    public void create() throws IOException {
        FSDataOutputStream fd = fs.create(new Path("/a.txt"));
        fd.writeBytes("123123");
        IOUtils.closeStream(fd);

    }

    @Test
    public void append() throws IOException {
        FSDataOutputStream append = fs.append(new Path("/a.txt"));
        append.writeBytes("aaaabbbb");

        IOUtils.closeStream(append);

    }
    @After
    public void  after() throws IOException {
        fs.close();
    }

}
