package com.deyu.hadoop.mr.friendstwo;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FFMapper1 extends Mapper<LongWritable, Text, Text, Text> {

    private Text k = new Text();
    private Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1 . key A, value 所关注的人
        String[] fields = value.toString().split(":");
        k.set(fields[0]);
        String[] friends = fields[1].split(",");
        for (String friend : friends) {
            v.set(friend);
            context.write(v, k);
        }
    }
}
