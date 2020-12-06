package com.test.wc;

import com.atguigu.utils.FileDeleteUtil;
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
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(WcDriver.class);

        job.setMapperClass(WcMapper.class);
        job.setReducerClass(WcReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        String outPath = "d:/temp/WcOutput";
        FileDeleteUtil.deleteFile(outPath);

        FileInputFormat.setInputPaths(job, new Path("d:/temp/week.txt"));
        FileOutputFormat.setOutputPath(job, new Path(outPath));

        boolean b = job.waitForCompletion(true);

        System.out.println(b? "Execute Success！" : "Execute Failed!");
    }
}
