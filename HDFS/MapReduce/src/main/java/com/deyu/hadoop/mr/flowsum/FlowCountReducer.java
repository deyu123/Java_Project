package com.deyu.hadoop.mr.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    long sumDownFlow = 0 ;
    long sumUpFlow  = 0;
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        for (FlowBean flowBean : values) {

            long downFlow = flowBean.getDownFlow();
            long upFlow = flowBean.getUpFlow();
            sumDownFlow += downFlow;
            sumUpFlow += upFlow;

        }

        FlowBean flowBean = new FlowBean(sumUpFlow, sumDownFlow);
        context.write(key, flowBean);

    }
}
