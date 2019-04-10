package com.deyu.hadoop.mr.ReduceJoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        TableBean pdBean = new TableBean();
        // 订单表可有很多
        ArrayList<TableBean> orderBeans = new ArrayList<>();
        for (TableBean tableBean : values) {

            if (tableBean.getFlag().contains("order")) {
                // 拷贝传递过来的每条订单数据到集合中
                TableBean orderBean = new TableBean();
                try {
                    BeanUtils.copyProperties(orderBean, tableBean);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                orderBeans.add(orderBean);
            } else {
                // 产品表
                try {
                    BeanUtils.copyProperties(pdBean, tableBean);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }

        for (TableBean bean : orderBeans) {
            bean.setPname(pdBean.getPname());
            context.write(bean, NullWritable.get());
        }

    }
}
