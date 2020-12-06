package com.atguigu.forgroup;

import com.atguigu.utils.FileDeleteUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class GroupDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(GroupDriver.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(GroupOutputFormat.class);

        job.setNumReduceTasks(0);
        FileDeleteUtil.deleteFile("d:/temp/nameOutput");
        FileInputFormat.setInputPaths(job, new Path("d:/temp/name.txt"));
        FileOutputFormat.setOutputPath(job, new Path("d:/temp/nameOutput"));

        boolean result = job.waitForCompletion(true);

        System.out.println(result? "执行成功！" : "执行失败...");
    }
}
