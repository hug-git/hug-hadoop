package com.atguigu.tool;

import org.apache.hadoop.util.Tool;

public class ToolFactory {
    public static Tool getTool(String name){
        Tool tool = null;
        switch (name) {
            case "wordcount":
                tool = new WcDriverTool();
                break;
            default:
                throw new RuntimeException("The Tool is not exit...");
        }
        return tool;
    }
}
