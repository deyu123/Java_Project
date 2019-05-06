package com.deyu.hbase.mr1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class Driver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        Job job = Job.getInstance(conf);
        job.setJarByClass(Driver.class);

        Scan scan = new Scan();

        TableMapReduceUtil.initTableMapperJob(
                "fruit",
                scan,
                ReadFruitMapper.class,
                ImmutableBytesWritable.class,
                Put.class,
                job
        );

        TableMapReduceUtil.initTableReducerJob(
                "fruit_mr",
                WriteFruitRuducer.class,
                job
        );

        job.waitForCompletion(true);
    }
}
