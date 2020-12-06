package com.atguigu.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable, Text,OrderBean, NullWritable> {
    private OrderBean orderBean = new OrderBean();
    private String fileName;

    @Override
    protected void setup(Context context) {
        InputSplit inputSplit = context.getInputSplit();
        FileSplit fileSplit = (FileSplit)inputSplit;
        fileName = fileSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        if ("order.txt".compareTo(fileName) == 0) {
            orderBean.setId(split[0]);
            orderBean.setPid(split[1]);
            orderBean.setAmount(Integer.parseInt(split[2]));
            orderBean.setPname("");
        } else {
            orderBean.setId("");
            orderBean.setPid(split[0]);
            orderBean.setAmount(0);
            orderBean.setPname(split[1]);
        }
        context.write(orderBean, NullWritable.get());
    }
}
