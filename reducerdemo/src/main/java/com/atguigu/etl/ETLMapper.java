package com.atguigu.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ETLMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    private Counter fail;
    private Counter pass;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
//        fail = context.getCounter("ETL", "FAIL");
//        pass = context.getCounter("ETL", "PASS");
        fail = context.getCounter(ETLStatus.FAILD );
        pass = context.getCounter(ETLStatus.PASS);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(" ");
        if (fields.length > 11) {
            pass.increment(1);
            context.write(value, NullWritable.get());
        } else {
            fail.increment(1);
        }
    }
}
