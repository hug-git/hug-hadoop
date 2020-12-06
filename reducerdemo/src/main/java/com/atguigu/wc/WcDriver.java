package com.atguigu.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WcDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://hadoop102:8020");
        configuration.set("mapreduce.framework.name","yarn");
        configuration.set("mapreduce.app-submission.cross-platform","true");
        configuration.set("yarn.resourcemanager.hostname","hadoop103");

        Job job = Job.getInstance(configuration);

//        job.setJarByClass(WcDriver.class);
        job.setJar("D:\\IdeaProjects\\hadoop\\reducerdemo\\target\\reducerdemo-1.0-SNAPSHOT.jar");

        job.setMapperClass(WcMapper.class);
        job.setReducerClass(WcReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

//        FileInputFormat.setInputPaths(job, new Path("d:/week.txt"));
//        FileOutputFormat.setOutputPath(job, new Path("d:/wcoutput"));
        FileInputFormat.setInputPaths(job, new Path("hdfs://hadoop102:8020/wcinput"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop102:8020/wcoutput"));

        boolean result = job.waitForCompletion(true);
        System.out.println(result ? 0 : 1);
    }
}
