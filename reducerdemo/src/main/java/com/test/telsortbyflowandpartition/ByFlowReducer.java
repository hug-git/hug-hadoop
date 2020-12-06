package com.test.telsortbyflowandpartition;

import com.test.telsumflow.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ByFlowReducer extends Reducer<FlowBean, Text,Text,FlowBean> {
    private FlowBean flowBean = new FlowBean();
    private Text phone = new Text();

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value, key);
        }
    }
}
