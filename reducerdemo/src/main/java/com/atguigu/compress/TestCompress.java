package com.atguigu.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

import java.io.IOException;

public class TestCompress {
    public static void main(String[] args) throws IOException {
        compress("d:/temp/hh.log", BZip2Codec.class);
    }

    @Test
    public void test() throws IOException {
        decompress("d:/temp/hh.log.bz2");
    }

    private static void decompress(String file) throws IOException {
        Configuration conf = new Configuration();

        //Get factory of compress format
        CompressionCodecFactory codecFactory = new CompressionCodecFactory(conf);
        CompressionCodec codec = codecFactory.getCodec(new Path(file));

        //Open the stream
        FileSystem fileSystem = FileSystem.get(conf);
        FSDataInputStream in = fileSystem.open(new Path(file));
        FSDataOutputStream out = fileSystem.create(new Path(
                file.substring(0, file.length() - codec.getDefaultExtension().length())
        ));

        //Pack the input stream created by fileSystem
        CompressionInputStream cin = codec.createInputStream(in);

        //Copies the input stream to output stream
        IOUtils.copyBytes(cin, out, 1024);

        //Closes all stream
        IOUtils.closeStreams(cin, out);
    }

    private static void compress(String file, Class<? extends CompressionCodec> codecClass) throws IOException {
        //Create a new object
        Configuration conf = new Configuration();
        CompressionCodec codec = ReflectionUtils.newInstance(codecClass, conf);

        //Open the stream
        FileSystem fileSystem = FileSystem.get(conf);
        FSDataInputStream in = fileSystem.open(new Path(file));
        FSDataOutputStream out = fileSystem.create(new Path(file + codec.getDefaultExtension()));

        //Pack the output stream created by fileSystem
        CompressionOutputStream count = codec.createOutputStream(out);

        //Copies the input stream to output stream
        IOUtils.copyBytes(in, out, 1024);

        //Closes all stream
//        IOUtils.closeStreams(in, out);
    }
}
