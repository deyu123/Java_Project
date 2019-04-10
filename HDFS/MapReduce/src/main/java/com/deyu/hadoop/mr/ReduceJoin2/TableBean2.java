package com.deyu.hadoop.mr.ReduceJoin2;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TableBean2 implements WritableComparable<TableBean2> {

    private String order_id; // 订单id
    private String p_id;      // 产品id
    private int amount;       // 产品数量
    private String pname;     // 产品名称
    private String flag;      // 表的标记

    public TableBean2() {
    }

    public String getOrder_id() {
        return order_id;
    }

    public void setOrder_id(String order_id) {
        this.order_id = order_id;
    }

    public String getP_id() {
        return p_id;
    }

    public void setP_id(String p_id) {
        this.p_id = p_id;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(order_id);
        out.writeUTF(p_id);
        out.writeInt(amount);
        out.writeUTF(pname);
        out.writeUTF(flag);

    }


    public void readFields(DataInput in) throws IOException {

        this.order_id = in.readUTF();
        this.p_id = in.readUTF();
        this.amount = in.readInt();
        this.pname = in.readUTF();
        this.flag = in.readUTF();

    }

    @Override
    public String toString() {
        return order_id + '\t' + p_id + '\t' + amount +
                '\t' + pname + '\t' +  flag;
    }

    @Override
    public int compareTo(TableBean2 o) {
        //先按照id 排序
        int i = this.p_id.compareTo(o.p_id);
        if(i==0){
            // 按照名字排序
            return o.pname.compareTo(this.pname);
        }else {
            return i;
        }
    }
}
