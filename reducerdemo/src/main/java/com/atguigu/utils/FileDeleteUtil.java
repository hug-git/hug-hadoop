package com.atguigu.utils;

import java.io.File;
import java.io.FileNotFoundException;

public class FileDeleteUtil {
    public static boolean deleteFile(String path) {
        File file = new File(path);
        return deleteAllFile(file);
    }
    private static boolean deleteAllFile(File file) {
        if (!file.exists()) {
//            throw new RuntimeException(file.getPath() + " is not exist");
            return false;
        }
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            for (File file1 : files) {
                deleteAllFile(file1);
            }
        }
        return file.delete();
    }
}
