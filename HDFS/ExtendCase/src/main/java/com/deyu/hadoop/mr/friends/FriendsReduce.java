package com.deyu.hadoop.mr.friends;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FriendsReduce extends Reducer<Text, Text, Text, NullWritable> {

    Text test = new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> list = new ArrayList<>();
        for (Text value : values) {
            String[] fields = value.toString().split(",");
            if(list.isEmpty()){
                for (String field : fields) {
                    list.add(field);
                }
            }else{
                String friends = "";
                for (String field : fields) {
                    boolean contains = list.contains(field);
                    if(contains){
                        // 有共同好友
                        friends = friends + field + " ";
                    }
                }
                if(friends != ""){
                    String result = key.toString() + "\t" + friends;
                    test.set(result);
                    context.write(test, NullWritable.get());
                }

            }
        }
    }
}
