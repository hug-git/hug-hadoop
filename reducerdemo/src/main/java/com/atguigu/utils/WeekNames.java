package com.atguigu.utils;

import java.io.*;
import java.util.Random;

public class WeekNames {
    public static void main(String[] args) throws Exception {
        Random random = new Random();
        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream("d:/temp/week.txt"));
        String[] weeks = {"Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"};
        int count = 0;
        for (int i = 0; i < 500000; i++) {
            try {
                bos.write((weeks[random.nextInt(7)] + " ").getBytes());
                count++;
                if (count >= (random.nextInt(20) + 2)) {
                    bos.write("\n".getBytes());
                    count = 0;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        bos.close();
    }
}
