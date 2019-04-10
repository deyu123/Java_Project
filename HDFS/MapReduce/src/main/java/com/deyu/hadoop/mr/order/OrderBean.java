package com.deyu.hadoop.mr.order;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {

    private int order_id; // 订单id号
    private double price; // 价格

    public OrderBean() {
    }

    public int getOrder_id() {
        return order_id;
    }

    public void setOrder_id(int order_id) {
        this.order_id = order_id;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    // 二次排序 ， 价格倒序排序
    @Override
    public int compareTo(OrderBean o) {
        int result;
        // 按照分组升序
        if(o.getOrder_id() > this.getOrder_id()){
            result = -1;
        }else if(o.getOrder_id() < this.getOrder_id()){
            result = 1;
        }else{
            // 按照 价格降序
            result = o.getPrice() > this.getPrice()? 1:-1;
        }
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.order_id);
        out.writeDouble(this.price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.order_id = in.readInt();
        this.price = in.readDouble();
    }

    @Override
    public String toString() {
        return "OrderBean{" +
                "order_id=" + order_id +
                ", price=" + price +
                '}';
    }
}

