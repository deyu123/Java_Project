package com.deyu.hadoop.mr.keyvaluetest;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class KVTextDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        // 设置切割符
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ");
        // 1 获取job对象
        Job job = Job.getInstance(conf);

        // 2 设置jar包位置，关联mapper和reducer
        job.setJarByClass(KVTextDriver.class);
        job.setMapperClass(KVTextMapper.class);
        job.setReducerClass(KVTextReducer.class);

        // 3 设置map输出kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // 4 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // 5 设置输入输出数据路径
//        FileInputFormat.setInputPaths(job, new Path(args[0]));

        FileInputFormat.setInputPaths(job, new Path("I:\\workspace\\java\\Java_Project\\HDFS\\MapReduce\\src\\main\\java\\com\\deyu\\hadoop\\mr\\keyvaluetest\\input\\aa.txt"));

        // 设置输入格式
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        // 6 设置输出数据路径
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        FileOutputFormat.setOutputPath(job, new Path("I:\\workspace\\java\\Java_Project\\HDFS\\MapReduce\\src\\main\\java\\com\\deyu\\hadoop\\mr\\keyvaluetest\\output"));

        // 7 提交job
        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);

    }
}
