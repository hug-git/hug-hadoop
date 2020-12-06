package com.atguigu.forgroup;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class GroupRecordWriter extends RecordWriter<LongWritable, Text> {
    private FSDataOutputStream bj;
    private FSDataOutputStream sh;
    private FSDataOutputStream sz;
    private FSDataOutputStream other;

    public void init(TaskAttemptContext job) throws IOException {
        FileSystem fileSystem = FileSystem.get(job.getConfiguration());
        bj = fileSystem.create(new Path(job.getConfiguration().get(FileOutputFormat.OUTDIR),"bj.txt"));
        sh = fileSystem.create(new Path(job.getConfiguration().get(FileOutputFormat.OUTDIR),"sh.txt"));
        sz = fileSystem.create(new Path(job.getConfiguration().get(FileOutputFormat.OUTDIR),"sz.txt"));
        other = fileSystem.create(new Path(job.getConfiguration().get(FileOutputFormat.OUTDIR),"other.txt"));
    }
    @Override
    public void write(LongWritable key, Text value) throws IOException {
//        System.out.println(value.toString());
        byte[] byt = (value.toString() + "\n").getBytes();
        String pre = value.toString().substring(0, 2);
        if ("BJ".equals(pre)) {
            bj.write(byt);
        } else if ("SH".equals(pre)) {
            sh.write(byt);
        } else if ("SZ".equals(pre)) {
            sz.write(byt);
        } else {
            other.write(byt);
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException {
        if (bj != null) {
            bj.close();
        }
        if (sh != null) {
            sh.close();
        }
        if (sz != null) {
            sz.close();
        }
        if (other != null) {
            other.close();
        }
    }
}
