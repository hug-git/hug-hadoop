package com.atguigu.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ETLDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(ETLDriver.class);

        job.setMapperClass(ETLMapper.class);
        job.setNumReduceTasks(0);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("d:/temp/web.log"));
        FileOutputFormat.setOutputPath(job, new Path("d:/temp/web-etl"));

        boolean result = job.waitForCompletion(true);

        System.out.println(result? "执行成功！" : "执行失败！");
    }
}
