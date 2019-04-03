package com.deyu.hadoop.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    Text k = new Text();
    IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
         // no super
        //1.获取一行
        String line = value.toString();
        //2.切割
        String[] str = line.split(" ");
        //3.输出
        for (String s : str) {
            k.set(s);
            context.write(k, v);
        }
    }
}
