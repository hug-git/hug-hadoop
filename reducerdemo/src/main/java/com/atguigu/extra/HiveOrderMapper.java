package com.atguigu.extra;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HiveOrderMapper extends Mapper<LongWritable, Text,IntWritable, NullWritable> {
    private IntWritable num = new IntWritable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        long length = context.getInputSplit().getLength();
        System.out.print("========================================================================= ");
        System.out.print("length of split = " + length);
        System.out.println(" =========================================================================");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        num.set(Integer.parseInt(value.toString()));
        context.write(num,NullWritable.get());
    }
}
