package com.deyu.hadoop.mr.MapJoin;


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    // 缓存小表
    Map<String, String> pdMap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        String path = cacheFiles[0].getPath();
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));

        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())) {
            String[] fields = line.split("\t");
            pdMap.put(fields[0], fields[1]);
        }
        reader.close();
    }

    Text text = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        1001	01	1
//        01	小米

        String line = value.toString();
        String[] fields = line.split("\t");
        String pid = fields[1];
        String pName = pdMap.get(pid);

        String lines = fields[0] + "\t" + fields[2] + "\t" +  pName;
        text.set(lines);

        context.write(text, NullWritable.get());
    }
}
