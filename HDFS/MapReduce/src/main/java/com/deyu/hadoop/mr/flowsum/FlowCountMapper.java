package com.deyu.hadoop.mr.flowsum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    Text text = new Text();
    FlowBean flowBean = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String word = value.toString();
        String[] fields = word.split("\t");
        String phonenum = fields[1];
        Long upFlow = Long.parseLong(fields[fields.length -3]);
        Long downFlow = Long.parseLong(fields[fields.length - 2]);

        text.set(phonenum);

        flowBean.setUpFlow(upFlow);
        flowBean.setDownFlow(downFlow);
        context.write(text, flowBean);

    }
}
