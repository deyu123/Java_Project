package com.deyu.hadoop.mr.inputformat;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class MyRecordReaderTest extends RecordReader<Text, BytesWritable> {
    private Text key = new Text();
    private BytesWritable value = new BytesWritable();
    private boolean isRead = false;
    private FSDataInputStream fsDataInputStream;
    private FileSplit fs;
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        fs = (FileSplit) split;
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        fsDataInputStream = fileSystem.open(fs.getPath());
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(!isRead){
            key.set(fs.getPath().toString());
            byte[] buffer = new byte[(int) fs.getLength()];
            fsDataInputStream.read(buffer);
            value.set(buffer, 0 , buffer.length);
            isRead = true;
            return true;
        }else {
            return false;
        }

    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return isRead?1:0;
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeStream(fsDataInputStream);
    }
}
