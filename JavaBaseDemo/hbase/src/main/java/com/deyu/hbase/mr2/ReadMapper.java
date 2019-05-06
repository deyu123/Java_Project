package com.deyu.hbase.mr2;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author liubo
 */
public class ReadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //1001	Apple	Red
        String[] split = value.toString().split("\t");
        byte[] rowKey = Bytes.toBytes(split[0]);
        byte[] infoName = Bytes.toBytes(split[1]);
        byte[] infoColor = Bytes.toBytes(split[2]);

        Put put = new Put(rowKey);
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), infoName);
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("color"), infoColor);

        ImmutableBytesWritable k = new ImmutableBytesWritable(rowKey);

        context.write(k, put);
    }
}
