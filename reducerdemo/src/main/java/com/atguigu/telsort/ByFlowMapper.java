package com.atguigu.telsort;

import com.atguigu.sumflow.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ByFlowMapper extends Mapper<LongWritable, Text, FlowBean,Text> {
    private Text phone = new Text();
    private FlowBean flowBean = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] info = value.toString().split("\t");
        int len = info.length;
        phone.set(info[1]);
        flowBean.setFlow(Long.parseLong(info[len-3]),Long.parseLong(info[len-2]));
        context.write(flowBean , phone);
    }
}
