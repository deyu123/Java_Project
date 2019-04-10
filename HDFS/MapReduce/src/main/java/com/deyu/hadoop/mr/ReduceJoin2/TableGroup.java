package com.deyu.hadoop.mr.ReduceJoin2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TableGroup extends WritableComparator {

    protected TableGroup(){
        super(TableBean2.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TableBean2 at = (TableBean2) a;
        TableBean2 bt = (TableBean2) b;
        return at.getP_id().compareTo(bt.getP_id());
    }
}


