package com.test.reducejoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ReJoinDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(ReJoinDriver.class);

        job.setMapperClass(ReJoinMapper.class);
        job.setReducerClass(ReJoinReducer.class);

        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.setGroupingComparatorClass(OrderGroup.class);

        FileInputFormat.setInputPaths(job, new Path("d:/temp/joinInput"));
        FileOutputFormat.setOutputPath(job, new Path("d:/temp/joinOutput"));

        boolean b = job.waitForCompletion(true);

        System.out.println(b ? 0 : 1);
    }
}
