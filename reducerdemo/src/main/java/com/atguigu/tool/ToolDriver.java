package com.atguigu.tool;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Arrays;

public class ToolDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        conf.set("fs.defaultFS", "hdfs://hadoop102:8020");
        conf.set("mapreduce.framework.name","yarn");
        conf.set("mapreduce.app-submission.cross-platform","true");
        conf.set("yarn.resourcemanager.hostname","hadoop103");

        Tool tool = ToolFactory.getTool(args[0]);

        ToolRunner.run(conf, tool, Arrays.copyOfRange(args, 1, args.length));
    }
}
