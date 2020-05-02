package com.example.faina.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;

public class InputUtils {


    /**
     * creates buffered file reader
     * @param clasz
     * @param fileName
     * @return buffered file reader
     * @throws FileNotFoundException
     */
    public static BufferedReader getReader(Class clasz, String fileName) throws FileNotFoundException {
        File file = new File(
                clasz.getClassLoader().getResource(fileName).getFile()
        );
        return new BufferedReader(new FileReader(file));
    }


    /**
     * finds out the input file name
     * @param args
     * @param defaultFileName
     * @return input file name
     */
    public static String getFileName(String[] args, String defaultFileName) {
        String fileName = defaultFileName;
        //override the file name with program argument
        if (args != null && args.length > 0 && args[0] != null && args[0].startsWith("input="))	{
            fileName = args[0];
        }
        return fileName;
    }
}
