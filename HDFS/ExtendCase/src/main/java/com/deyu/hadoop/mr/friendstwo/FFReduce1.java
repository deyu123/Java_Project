package com.deyu.hadoop.mr.friendstwo;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class FFReduce1 extends Reducer<Text, Text, Text, NullWritable> {
    private StringBuilder sb = new StringBuilder();

    private Text text = new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        sb.delete(0, sb.length());
        for (Text value : values) {
            sb.append(value.toString()).append(",");
        }
        String Friendpp = key.toString() + "\t" + sb.toString();
        text.set(Friendpp);
        context.write(text , NullWritable.get());
    }
}
