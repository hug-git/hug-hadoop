package com.atguigu.mapjoin;

import com.atguigu.reducejoin.OrderBean;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
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
import java.util.HashMap;
import java.util.Map;

public class MJMapper extends Mapper<LongWritable, Text,OrderBean, NullWritable> {
    private Map<String,String> pdMap = new HashMap<>();
//    private Text result = new Text();
    private OrderBean orderBean = new OrderBean();
    @Override
    protected void setup(Context context) throws IOException {

        //Read distributed cache
        URI[] files = context.getCacheFiles();

        //Open the stream
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        FSDataInputStream cache = fileSystem.open(new Path(files[0]));

        //Convert to Character Stream
        BufferedReader br = new BufferedReader(new InputStreamReader(cache));

        //Handle by line
        String line;
        while (StringUtils.isNotEmpty(line = br.readLine())){
            String[] fields = line.split("\t");
            pdMap.put(fields[0],fields[1]);
        }

        br.close();
        fileSystem.close();

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
//        result.set(split[0] + "\t" + pdMap.getOrDefault(split[1],"无品牌") + "\t" + split[2]);
//        context.write(result, NullWritable.get());
        orderBean.setId(split[0]);
        orderBean.setPname(pdMap.getOrDefault(split[1],"无品牌"));
        orderBean.setAmount(Integer.parseInt(split[2]));
        context.write(orderBean,NullWritable.get());
    }

}
