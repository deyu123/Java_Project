package com.deyu.hadoop.mr.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner extends Partitioner<Text, FlowBean> {

    @Override
    public int getPartition(Text phoneNum, FlowBean flowBean, int numPartitions) {
        String prePhoneNum = phoneNum.toString().substring(0, 3);
        int result = 4;
        if ("136".equals(prePhoneNum)) {
            result = 0;
        } else if ("137".equals(prePhoneNum)){
            result = 1;
        }else if ("138".equals(prePhoneNum)){
            result = 2;
        }else if ("139".equals(prePhoneNum)) {
            result = 3;
        }

        return result;
    }
}
