package com.deyu.hadoop.mr.flowsum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1. 得到实例和 类路径
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(FlowCountDriver.class);
        job.setReducerClass(FlowCountReducer.class);
        job.setMapperClass(FlowCountMapper.class);

        //2. 设置 map 输出和 kv 输出的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //3. 输入，输出的地址
        FileInputFormat.setInputPaths(job, new Path("e:/input/phone_data.txt"));
        FileOutputFormat.setOutputPath(job, new Path("e:/output"));

        //4. sumbit
        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);


    }
}
