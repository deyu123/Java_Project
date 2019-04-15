package com.deyu.hadoop.mr.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;


public class TestCompress {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
//        compress("E:\\input\\aa.txt", "org.apache.hadoop.io.compress.BZip2Codec");
//        compress("E:\\input\\aa.txt", "org.apache.hadoop.io.compress.GzipCodec");
//        compress("E:\\input\\aa.txt", "org.apache.hadoop.io.compress.DefaultCodec");
//        descompress("E:\\input\\aa.txt.gz");
        descompress("E:\\input\\aa.txt.bz2");
    }

    private static void descompress(String fileName) throws IOException {
        // 判断是否能够解压
        CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
        CompressionCodec codec = factory.getCodec(new Path(fileName));

        if(codec==null){
            System.out.println("can not decompress");
            return;
        }
        // 打开输入流
        CompressionInputStream cis = codec.createInputStream(new FileInputStream(new File(fileName)));

        //输出流
        // 可以解压改名，只是流路径变了
        FileOutputStream fos = new FileOutputStream(new File(fileName + ".decoded"));
        //流对拷
        IOUtils.copyBytes(cis, fos,10*1024*1024, false);
        //关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(cis);

    }

    private static void compress(String fileName, String clsName) throws ClassNotFoundException, IOException {

        //1. 开启输入流
        FileInputStream fis = new FileInputStream(new File(fileName));
        Class aClass = Class.forName(clsName);
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(aClass, new Configuration());

        //2. 开启输出流
        FileOutputStream fos = new FileOutputStream(new File(fileName + codec.getDefaultExtension()));
        CompressionOutputStream cos = codec.createOutputStream(fos);

        //3. 进行流对拷
        IOUtils.copyBytes(fis, cos, 5*1024*1024, false);

        //4. 关闭流资源
        IOUtils.closeStream(cos);
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);


    }
}
