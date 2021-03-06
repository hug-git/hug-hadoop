package com.test.reducejoin;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderGroup extends WritableComparator {
    protected OrderGroup() {
        super(OrderBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean o1 = (OrderBean)a;
        OrderBean o2 = (OrderBean)b;
        return o1.getPid().compareTo(o2.getPid());
    }
}
