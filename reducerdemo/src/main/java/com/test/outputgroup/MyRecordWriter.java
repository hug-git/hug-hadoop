package com.test.outputgroup;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyRecordWriter extends RecordWriter<LongWritable,Text> {
    private FSDataOutputStream sh;
    private FSDataOutputStream bj;
    private FSDataOutputStream sz;
    private FSDataOutputStream other;
    private Configuration conf;

    public void init(TaskAttemptContext job) throws IOException {
        conf = job.getConfiguration();
        FileSystem fileSystem = FileSystem.get(conf);
        sh = fileSystem.create(new Path(conf.get(FileOutputFormat.OUTDIR),"sh.txt"));
        bj = fileSystem.create(new Path(conf.get(FileOutputFormat.OUTDIR),"bj.txt"));
        sz = fileSystem.create(new Path(conf.get(FileOutputFormat.OUTDIR),"sz.txt"));
        other = fileSystem.create(new Path(conf.get(FileOutputFormat.OUTDIR),"other.txt"));
    }
    @Override
    public void write(LongWritable key, Text value) throws IOException, InterruptedException {
        String pre = value.toString().substring(0, 2);
        byte[] info = (value.toString() + "\n").getBytes();
        switch (pre) {
            case "SH":
                sh.write(info);
                break;
            case "BJ":
                bj.write(info);
                break;
            case "SZ":
                sz.write(info);
                break;
            default:
                other.write(info);
                break;
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        if (sh != null) {
            sh.close();
        }
        if (bj != null) {
            bj.close();
        }
        if (sz != null) {
            sz.close();
        }
        if (other != null) {
            other.close();
        }
    }

}
