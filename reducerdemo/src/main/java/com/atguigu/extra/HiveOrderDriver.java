package com.atguigu.extra;

import com.atguigu.utils.FileDeleteUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;

public class HiveOrderDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(HiveOrderDriver.class);

        job.setMapperClass(HiveOrderMapper.class);
        job.setReducerClass(HiveOrderReducer.class);

        job.setCombinerClass(HiveOrderCombiner.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        FileDeleteUtil.deleteFile("d:/orderNum");
        FileInputFormat.setInputPaths(job, new Path("d:/num.txt"));
        FileOutputFormat.setOutputPath(job, new Path("d:/orderNum"));

        long be = new Date().getTime();
        boolean b = job.waitForCompletion(true);
        long af = new Date().getTime();

        System.out.println(b ? 0 : 1);
        System.out.println(af - be);
    }

}
