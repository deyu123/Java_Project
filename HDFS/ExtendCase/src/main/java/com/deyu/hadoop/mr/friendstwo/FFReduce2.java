package com.deyu.hadoop.mr.friendstwo;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FFReduce2 extends Reducer<Text, Text, Text, NullWritable> {
    private StringBuilder sb = new StringBuilder();
    private Text k = new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        sb.delete(0, sb.length());
        for (Text value : values) {
            sb.append(value).append(" ");
        }
        String result = key.toString() + "\t" + sb.toString();
        k.set(result);
        context.write(k, NullWritable.get());
    }
}
