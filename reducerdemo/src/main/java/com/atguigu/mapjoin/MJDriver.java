package com.atguigu.mapjoin;

import com.atguigu.reducejoin.OrderBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

public class MJDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(MJDriver.class);

        job.setMapperClass(MJMapper.class);

//        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(0);
        job.addCacheFile(URI.create("file:///d:/temp/joininput/pd.txt"));

        FileInputFormat.setInputPaths(job, new Path("d:/temp/joininput/order.txt"));
        FileOutputFormat.setOutputPath(job, new Path("d:/temp/MapJoinOutput"));

        boolean result = job.waitForCompletion(true);

        System.out.println(result ? "执行成功！" : "执行失败！");
    }
}
