package com.deyu.hadoop.mr.Index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FirstIndexDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(FirstIndexDriver.class);
        job.setMapperClass(FirstIndexMapper.class);
        job.setReducerClass(FirstIndexReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job , new Path("I:\\workspace\\java\\Java_Project\\HDFS\\ExtendCase\\src\\main\\java\\com\\deyu\\hadoop\\mr\\Index\\input"));
        FileOutputFormat.setOutputPath(job , new Path("I:\\workspace\\java\\Java_Project\\HDFS\\ExtendCase\\src\\main\\java\\com\\deyu\\hadoop\\mr\\Index\\output"));

        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);

    }
}
