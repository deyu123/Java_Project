package com.deyu.hadoop.mr.friends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FriendsDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(FriendsDriver.class);
        job.setMapperClass(FriendsMapper.class);
        job.setReducerClass(FriendsReduce.class);

        job.setInputFormatClass(MyInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(2);
        FileInputFormat.setInputPaths(job, new Path("I:\\workspace\\java\\Java_Project\\HDFS\\ExtendCase\\src\\main\\java\\com\\deyu\\hadoop\\mr\\friends\\input"));
        FileOutputFormat.setOutputPath(job, new Path("I:\\workspace\\java\\Java_Project\\HDFS\\ExtendCase\\src\\main\\java\\com\\deyu\\hadoop\\mr\\friends\\output"));

        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
