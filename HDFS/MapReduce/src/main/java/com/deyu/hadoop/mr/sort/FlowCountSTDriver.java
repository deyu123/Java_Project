package com.deyu.hadoop.mr.sort;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowCountSTDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(FlowCountSTDriver.class);
        job.setMapperClass(FlowCountSTMapper.class);
        job.setReducerClass(FlowCountSTReducer.class);

        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        FileInputFormat.setInputPaths(job, new Path("I:\\workspace\\java\\Java_Project\\HDFS\\MapReduce\\src\\main\\java\\com\\deyu\\hadoop\\mr\\sort\\input\\phone_data.txt"));
        FileOutputFormat.setOutputPath(job, new Path("I:\\workspace\\java\\Java_Project\\HDFS\\MapReduce\\src\\main\\java\\com\\deyu\\hadoop\\mr\\sort\\output"));

        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }
}






