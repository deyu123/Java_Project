package com.deyu.hadoop.mr.ReduceJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {

    String name;
    TableBean tableBean = new TableBean();
    Text k = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) context.getInputSplit();
        name = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        if(name.contains("order")){
//            1001	01	1
//            01	小米
            tableBean.setOrder_id(fields[0]);
            tableBean.setP_id(fields[1]);
            tableBean.setAmount(Integer.parseInt(fields[2]));
            tableBean.setFlag("order");
            tableBean.setPname("");

            k.set(fields[1]);
        }else {
            tableBean.setP_id(fields[0]);
            tableBean.setPname(fields[1]);
            tableBean.setFlag("pd");
            tableBean.setAmount(0);
            tableBean.setOrder_id("");

            k.set(fields[0]);
        }

        context.write(k, tableBean);
    }
}
