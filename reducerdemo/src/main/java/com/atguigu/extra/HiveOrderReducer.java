package com.atguigu.extra;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class HiveOrderReducer extends Reducer<IntWritable, NullWritable, IntWritable, NullWritable> {
    private int count = 0;

    @Override
    protected void reduce(IntWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<NullWritable> it = values.iterator();
        while (count < 30 && it.hasNext()) {
            it.next();
            System.out.println(count + " reducer-----------------" + key);
            context.write(key, NullWritable.get());
            count++;
        }
    }
}
