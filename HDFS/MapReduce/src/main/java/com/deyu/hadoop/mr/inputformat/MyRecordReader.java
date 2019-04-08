package com.deyu.hadoop.mr.inputformat;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class MyRecordReader extends RecordReader<Text, BytesWritable> {
    //        1.保证不能被切片
    //        2.重写isSplitable
    //        3.每组只返回一组key, value 值
    //        4.

    private boolean isRead = false;
    private Text key = new Text();
    private BytesWritable value = new BytesWritable();
    private FSDataInputStream fsDataInputStream ;
    private FileSplit fs ;


    /**
     * 初始化
     *
     * @param split
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        fs = (FileSplit) split;
//        获取文件的path
        Path path = fs.getPath();
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        fsDataInputStream = fileSystem.open(path);

    }

    /**
     * 得到下一个kv
     *
     * @return true 表示读到了
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        if (!isRead) {
//            1.读文件，读到key, value
//            2.读key
            key.set(fs.getPath().toString());
//            3.读value
            byte[] buffer = new byte[(int) fs.getLength()];
            fsDataInputStream.read(buffer);
            value.set(buffer,0 , buffer.length);
            isRead = true;
            return true;

        } else {
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

    /**
     * 进度条
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return isRead ? 1 : 0;
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeStream(fsDataInputStream);
    }
}
