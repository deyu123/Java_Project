package com.deyu.hadoop.mr.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountSTMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
    private Text k = new Text();
    private FlowBean flowBean = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] fields = value.toString().split("\t");
        k.set(fields[0]);
        long upFlow = Long.parseLong(fields[1]);
        long downFlow = Long.parseLong(fields[2]);
        flowBean.set(downFlow, upFlow);
        context.write(flowBean, k);
    }
}
