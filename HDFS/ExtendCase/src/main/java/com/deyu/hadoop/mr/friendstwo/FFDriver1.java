package com.deyu.hadoop.mr.friendstwo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FFDriver1 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(FFDriver1.class);
        job.setMapperClass(FFMapper1.class);
        job.setReducerClass(FFReduce1.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("I:\\workspace\\java\\Java_Project\\HDFS\\ExtendCase\\src\\main\\java\\com\\deyu\\hadoop\\mr\\friendsFirst\\input"));
        FileOutputFormat.setOutputPath(job, new Path("I:\\workspace\\java\\Java_Project\\HDFS\\ExtendCase\\src\\main\\java\\com\\deyu\\hadoop\\mr\\friendsFirst\\ouput"));

        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }
}

