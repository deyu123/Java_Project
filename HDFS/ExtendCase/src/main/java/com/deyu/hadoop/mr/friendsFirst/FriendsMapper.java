package com.deyu.hadoop.mr.friendsFirst;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FriendsMapper extends Mapper<Text, Text, Text, Text> {

    List<String > list = new ArrayList<>();
    Text testKey = new Text();
    Text testValue1 = new Text();
    Text testValue2 = new Text();

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("key:" + key.toString());
        System.out.println("value:" + value.toString());
        String v= value.toString();
        if(v.trim() != "") {
            System.out.println(v);
            list.add(v);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        System.out.println(list);
        for(int i = 0 ; i < list.size()-1; i++){
            System.out.println("fi:" + i + list.get(i));
            String[] fi = list.get(i).split(":");
            for(int j=i +1;j < list.size(); j ++){
                System.out.println("fj:" + j +list.get(j));
                String[] fj = list.get(j).split(":");
                String a_b = fi[0] + "-" + fj[0];
//                String a_b_astr = fi[0] + "-" + fj[0] + ":" + fi[0];
////                String a_b_bstr = fi[0] + "-" + fj[0] + ":" + fj[0];
////                test1.set(a_b_astr);
////                test2.set(a_b_bstr);
////                context.write(test1);
////                context.write(test2);
                testKey.set(a_b);
                testValue1.set(fi[1]);
                testValue2.set(fj[1]);
                context.write(testKey, testValue1);
                context.write(testKey, testValue2);

            }
        }
    }
}
