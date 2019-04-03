package com.deyu.hadoop.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    int sum ;
    IntWritable v = new IntWritable();


    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // no super
        //1. 累计求和
        sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        //2. 输出

        v.set(sum);
        context.write(key, v);

    }
}
