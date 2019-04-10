package com.deyu.hadoop.mr.ReduceJoin2;

import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;


public class TableReducer2 extends Reducer<TableBean2, NullWritable, NullWritable, TableBean2> {

    @Override
    protected void reduce(TableBean2 key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
//        map 整合后，得到一张表，然后另外输出
//        key1:	01	0	小米	pd
//        key2:1004	01	4		order
//        key2:1001	01	1		order
//        key1:	02	0	华为	pd
//        key2:1005	02	5		order
//        key2:1002	02	2		order
//        key1:	03	0	格力	pd
//        key2:1006	03	6		order
//        key2:1003	03	3		order

        Iterator<NullWritable> iterator = values.iterator();

        iterator.next();
        String pname = key.getPname();
        System.out.println("key1:"+key.toString());

        while (iterator.hasNext()){
            iterator.next();
            System.out.println("key2:" + key.toString());
            key.setPname(pname);
            context.write(NullWritable.get(), key);
        }



    }
}
