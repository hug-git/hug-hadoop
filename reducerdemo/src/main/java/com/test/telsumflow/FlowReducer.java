package com.test.telsumflow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text,FlowBean,Text,FlowBean> {
    private FlowBean flowBean = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        Long inFlow = 0L;
        Long outFlow = 0L;
        for (FlowBean value : values) {
            inFlow += value.getInFlow();
            outFlow += value.getOutFlow();
        }
        flowBean.setFlow(inFlow, outFlow);
        context.write(key, flowBean);
    }
}
