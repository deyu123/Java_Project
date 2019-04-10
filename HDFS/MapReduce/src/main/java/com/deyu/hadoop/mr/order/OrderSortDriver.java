package com.deyu.hadoop.mr.order;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class OrderSortDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(OrderSortDriver.class);

        job.setMapperClass(OrderSortMapper.class);
        job.setReducerClass(OrderSortReducer.class);
        job.setGroupingComparatorClass(OrderSortGroupingComparator.class);

        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("I:\\workspace\\java\\Java_Project\\HDFS\\MapReduce\\src\\main\\java\\com\\deyu\\hadoop\\mr\\order\\input\\GroupingComparator.txt"));
        FileOutputFormat.setOutputPath(job, new Path("I:\\workspace\\java\\Java_Project\\HDFS\\MapReduce\\src\\main\\java\\com\\deyu\\hadoop\\mr\\order\\output"));

        boolean b = job.waitForCompletion(true);
        System.exit(b?1:0);
    }
}
