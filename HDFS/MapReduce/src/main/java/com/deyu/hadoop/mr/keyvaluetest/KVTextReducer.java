package com.deyu.hadoop.mr.keyvaluetest;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KVTextReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    LongWritable v = new LongWritable();
    long sum ;
    
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        for (LongWritable v : values) {
            sum += v.get();
        }
        v.set(sum);
        context.write(key, v);

    }
}
