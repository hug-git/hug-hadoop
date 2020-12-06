package com.atguigu.telsort;

import com.atguigu.sumflow.FlowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ByFlowDriver {
    public static void main(String[] args) throws Exception {

        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(ByFlowDriver.class);

        job.setMapperClass(ByFlowMapper.class);
        job.setReducerClass(ByFlowReducer.class);

        job.setPartitionerClass(ByFlowPartition.class);
        job.setNumReduceTasks(5);

        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job, new Path("d:/temp/phone_data.txt"));
        FileOutputFormat.setOutputPath(job, new Path("d:/temp/z_phoneSort"));

        boolean result = job.waitForCompletion(true);

        System.out.println(result ? 0 : 1);
    }
}
