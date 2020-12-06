package com.atguigu.reducejoin;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyGroupComparator extends WritableComparator {
    protected MyGroupComparator() {
        super(OrderBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        if (a instanceof OrderBean && b instanceof OrderBean) {
            OrderBean o1 = (OrderBean)a;
            OrderBean o2 = (OrderBean)b;
            return o1.getPid().compareTo(o2.getPid());
        }
        throw new RuntimeException("传入的对象非OrderBean类！");
    }
}
