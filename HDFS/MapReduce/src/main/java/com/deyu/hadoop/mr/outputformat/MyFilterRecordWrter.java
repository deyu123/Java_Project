package com.deyu.hadoop.mr.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyFilterRecordWrter extends RecordWriter<Text, NullWritable> {
    private FileSystem fs;
    private FSDataOutputStream atguiguFSO;
    private FSDataOutputStream otherFSO;

    public MyFilterRecordWrter(TaskAttemptContext job) {
        try {
            Configuration configuration = job.getConfiguration();
            String parent = configuration.get(FileOutputFormat.OUTDIR);
            fs = FileSystem.get(configuration);
//            Path path1 = new Path("I:\\workspace\\java\\Java_Project\\HDFS\\MapReduce\\src\\main\\java\\com\\deyu\\hadoop\\mr\\outputformat\\output\\atguigu.txt");
//            Path path2 = new Path("I:\\workspace\\java\\Java_Project\\HDFS\\MapReduce\\src\\main\\java\\com\\deyu\\hadoop\\mr\\outputformat\\output\\other.txt");
            Path path1 = new Path(parent + "/atguigu.txt");
            Path path2 = new Path(parent + "/other.txt");
            atguiguFSO = fs.create(path1);
            otherFSO = fs.create(path2);

        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {

        if (key.toString().contains("atguigu")) {
            atguiguFSO.write(key.toString().getBytes());
        } else {
            otherFSO.write(key.toString().getBytes());
        }

    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(atguiguFSO);
        IOUtils.closeStream(otherFSO);
    }
}
