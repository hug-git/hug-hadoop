package com.test.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

public class MapJoinDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(MapJoinDriver.class);

        job.setMapperClass(JoinMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(0);
        job.addCacheFile(URI.create("file:///d:/temp/joinInput/pd.txt"));

        FileInputFormat.setInputPaths(job, new Path("d:/temp/joinInput/order.txt"));
        FileOutputFormat.setOutputPath(job, new Path("d:/temp/joinOutput"));

        boolean b = job.waitForCompletion(true);

        System.out.println(b ? 0 : 1);
    }
}
