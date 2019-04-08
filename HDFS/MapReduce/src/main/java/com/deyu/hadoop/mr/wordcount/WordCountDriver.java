package com.deyu.hadoop.mr.wordcount;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;

public class WordCountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.获取配置信息和封装任务
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 增加配置, 增加四个小文件切片为 4， 设置切片规则CombineTextInputFormat后， 切片为1
        // 如果不设置InputFormat，它默认用的是TextInputFormat.class
        job.setInputFormatClass(CombineTextInputFormat.class);
        //虚拟存储切片最大值设置4m
        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);

        //2. 设置jar 加载路径
        job.setJarByClass(WordCountDriver.class);

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReduce.class);

        //4. 设置 map 输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //5. 设置最终输出的k,v 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //6 . 设置输入地址，输出的地址
//        FileInputFormat.setInputPaths(job, new Path("e:/input/aa.txt"));
        FileInputFormat.setInputPaths(job, new Path(args[0]),new Path(args[1]),new Path(args[2]),new Path(args[3]));
//        FileOutputFormat.setOutputPath(job, new Path("e:/output"));
        FileOutputFormat.setOutputPath(job, new Path(args[4]));
//        FileInputFormat.setInputPaths(new JobConf(), path);
//        FileOutputFormat.setOutputPath(new JobConf(conf), new Path(""));

        //7. 提交
//        job.submit();
        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);


    }
}
