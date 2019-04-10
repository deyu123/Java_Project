package com.deyu.hadoop.mr.order;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class OrderSortReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {

    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
//        将所有的值写出
//        for (NullWritable value : values) {
//            context.write(key, value);
//        }
        //只求最大值
//        context.write(key,NullWritable.get());
//         求前两名
        int i = 0;
        Iterator<NullWritable> iterator = values.iterator();
        while (iterator.hasNext()) {
            iterator.next();
            if (i < 2) {
                System.out.println("i : " + i );
                context.write(key, NullWritable.get());
            }
            i++;
        }
    }
}


