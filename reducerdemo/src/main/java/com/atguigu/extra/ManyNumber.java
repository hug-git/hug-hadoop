package com.atguigu.extra;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.util.Random;

public class ManyNumber {
    public static void main(String[] args) throws Exception {
        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream("d:/num.txt"));
        Random random = new Random();
        for (int i = 0; i < 10000000; i++) {
            String num = random.nextInt(5000000) + 5000 + "\n";
            bos.write(num.getBytes());
        }
        bos.close();
    }
}
