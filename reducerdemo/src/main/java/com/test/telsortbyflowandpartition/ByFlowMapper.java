package com.test.telsortbyflowandpartition;

import com.test.telsumflow.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ByFlowMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
    private FlowBean flowBean = new FlowBean();
    private Text phone = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        int len = split.length;
        phone.set(split[1]);
        flowBean.setFlow(Long.parseLong(split[len - 3]),Long.parseLong(split[len - 2]));
        context.write(flowBean, phone);
    }
}
