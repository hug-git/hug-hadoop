package com.test.reducejoin;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class ReJoinReducer extends Reducer<OrderBean, NullWritable,OrderBean,NullWritable> {
    private OrderBean orderBean = new OrderBean();
    private String pname;
    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        Iterator iterator = values.iterator();
        iterator.next();
        pname = key.getPname();
        while (iterator.hasNext()) {
            iterator.next();
            key.setPname(pname);
            context.write(key, NullWritable.get());
        }
    }
}
