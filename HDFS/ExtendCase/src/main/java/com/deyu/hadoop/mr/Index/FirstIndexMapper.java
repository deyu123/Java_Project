package com.deyu.hadoop.mr.Index;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class FirstIndexMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    String name;
    String wordFileName;
    Text kv = new Text();
    IntWritable num = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split(" ");
        //获取文件名
        FileSplit split = (FileSplit) context.getInputSplit();
        name = split.getPath().getName();
        for (String word : words) {
            wordFileName = word + "\t" + name;
            kv.set(wordFileName);
            context.write(kv, num);
        }
    }
}
