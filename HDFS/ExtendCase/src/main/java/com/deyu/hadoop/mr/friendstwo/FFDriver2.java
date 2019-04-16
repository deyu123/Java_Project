package com.deyu.hadoop.mr.friendstwo;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class FFDriver2 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, "\t");
        Job job = Job.getInstance(conf);
        job.setJarByClass(FFDriver2.class);
        job.setMapperClass(FFMapper2.class);
        job.setReducerClass(FFReduce2.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        FileInputFormat.setInputPaths(job, new Path("I:\\workspace\\java\\Java_Project\\HDFS\\ExtendCase\\src\\main\\java\\com\\deyu\\hadoop\\mr\\friendsFirst\\ouput\\part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path("I:\\workspace\\java\\Java_Project\\HDFS\\ExtendCase\\src\\main\\java\\com\\deyu\\hadoop\\mr\\friendsFirst\\ouput1"));

        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);

    }
}
