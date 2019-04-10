package com.deyu.hadoop.mr.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements WritableComparable<FlowBean> {

    private long downFlow;
    private long upFlow;
    private long sumFlow;

    public FlowBean() {
    }

    public void set(long downFlow, long upFlow) {
        this.downFlow = downFlow;
        this.upFlow = upFlow;
        sumFlow = downFlow + upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }


    @Override
    public String toString() {
        return downFlow + "\t" + upFlow + "\t" + sumFlow;
    }

    @Override
    public int compareTo(FlowBean flowBean) {
        // 按照总流量大小倒序排列
        int result = 0;
        if (flowBean.getSumFlow() > sumFlow) {
            result = 1;
        } else if(flowBean.getSumFlow() < sumFlow) {
            result = -1;
        }
        return result;
    }

    /**
     * 序列化
     *
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    /**
     * 反序列化
     *
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readLong();
        this.downFlow = in.readLong();
        this.sumFlow = in.readLong();
    }
}
