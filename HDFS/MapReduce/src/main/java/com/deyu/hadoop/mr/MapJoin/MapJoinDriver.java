package com.deyu.hadoop.mr.MapJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MapJoinDriver {
    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(MapJoinDriver.class);
        job.setMapperClass(MapJoinMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setCacheFiles(new URI[]{new URI("file:///I:/workspace/java/Java_Project/HDFS/MapReduce/src/main/java/com/deyu/hadoop/mr/MapJoin/input/pd.txt")});
        job.setNumReduceTasks(0);
        FileInputFormat.setInputPaths(job, new Path("I:\\workspace\\java\\Java_Project\\HDFS\\MapReduce\\src\\main\\java\\com\\deyu\\hadoop\\mr\\MapJoin\\input\\order.txt"));
        FileOutputFormat.setOutputPath(job, new Path("I:\\workspace\\java\\Java_Project\\HDFS\\MapReduce\\src\\main\\java\\com\\deyu\\hadoop\\mr\\MapJoin\\output"));
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
