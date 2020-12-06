package com.test.mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

public class JoinMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    private Map<String,String> pdMap = new HashMap<>();
    private Text order = new Text();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] files = context.getCacheFiles();

        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        FSDataInputStream open = fileSystem.open(new Path(files[0]));

        BufferedReader br = new BufferedReader(new InputStreamReader(open));

        String line;
        while (StringUtils.isNotEmpty(line = br.readLine())){
            String[] fields = line.split("\t");
            pdMap.put(fields[0],fields[1]);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        order.set(split[0] + "\t" + pdMap.get(split[1]) + "\t" + split[2]);
        context.write(order, NullWritable.get());
    }
}
