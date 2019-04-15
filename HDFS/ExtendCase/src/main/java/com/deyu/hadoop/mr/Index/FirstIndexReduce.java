package com.deyu.hadoop.mr.Index;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FirstIndexReduce extends Reducer<Text, IntWritable, Text, NullWritable> {

    IntWritable num = new IntWritable();

    Text k = new Text();
    String keyName = "";
    String valueName = "";
    List<String> sumList = new ArrayList<String>();
    String name;
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }

//        num.set(sum);
//        context.write(key, num);



        name = key + "\t" + sum;
        sumList.add(name);
//                String[] fields = key.toString().split("\t");
//        if (keyName == "") {
//            keyName = fields[0];
//            valueName = fields[0] + " " + fields[1] + " " + sum + " ";
//            System.out.println("=" + valueName);
//        } else if (keyName.equals(fields[0])) {
//            valueName +=  fields[1] + " " + sum + "";
//            System.out.println("+=" + valueName);
//        } else {
//            System.out.println(valueName);
//            k.set(valueName);
//            context.write(k, NullWritable.get());
//            keyName = fields[0];
//            valueName = fields[0] + " " + fields[1] + " " + sum + " ";
//        }

    }



    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println(sumList);
        String keyName = "";
        String valueName  ="";
        for (String str : sumList) {
            String[] fields = str.split("\t");

            if(keyName == ""){
                keyName = fields[0];
                valueName = fields[0] + " " + fields[1] + " " + fields[2] + " ";
                System.out.println("=" + valueName);
            }else if(keyName.equals(fields[0])){
                valueName = valueName + fields[1] + " " + fields[2] + " ";
                System.out.println("+=" + valueName);
            }else {
                System.out.println(valueName);
                k.set(valueName);
                keyName = fields[0];
                valueName = fields[0] + " " + fields[1] + " " + fields[2] + " ";
                context.write(k, NullWritable.get());
            }

        }
        k.set(valueName);
        context.write(k, NullWritable.get());
    }
}

