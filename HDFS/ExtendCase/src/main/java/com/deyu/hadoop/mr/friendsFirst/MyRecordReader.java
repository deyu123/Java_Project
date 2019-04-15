package com.deyu.hadoop.mr.friendsFirst;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class MyRecordReader extends RecordReader<Text, Text> {

    //        1.保证不能被切片
    //        2.重写isSplitable
    //        3.每组只返回一组key, value 值

    private boolean isRead = false;
    private Text key = new Text();
    private List<String> list = new ArrayList<>();
    private FSDataInputStream fsDataInputStream ;
    private FileSplit fs ;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        fs = (FileSplit) split;
//        获取文件的path
        Path path = fs.getPath();
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        fsDataInputStream = fileSystem.open(path);

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!isRead) {
//            1.读文件，读到key, value
//            2.读key
            key.set("filename");
//            key.set(fs.getPath().toString());
//            3.读value
            String path = fs.getPath().toString();
            String append = "";
            URI uri = null;
            try {
                uri = new URI(path);
            } catch (URISyntaxException e) {
                e.printStackTrace();
            }
            System.out.println(uri);
            BufferedReader br = new BufferedReader(new FileReader(new File(uri)));
            String line;
            while ((line = br.readLine()) != null) {
                append = append + "->" +line;
            }

            System.out.println("buffer:" + append);
            list.add(append);
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
    public Text getCurrentValue() throws IOException, InterruptedException {
        String append = "";
        for (String s : list) {
            append = append + "-" + s;
        }
        return new Text(append);
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return isRead ? 1 : 0;
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeStream(fsDataInputStream);
    }
}
