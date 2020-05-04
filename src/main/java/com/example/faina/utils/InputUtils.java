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
     * @return default input file name if not overridden by program params
     */
    public static String getFileName(String[] args, String defaultFileName) {
        String fileName = defaultFileName;
        String inputPrefix = "input=";
        //override the file name with program argument
        if (args != null)   {
            for (String arg: args)  {
                if (arg != null && arg.startsWith(inputPrefix))    {
                    fileName = arg.split(inputPrefix)[1];
                    break;
                }
            }
        }
        return fileName;
    }

    /**
     * gets path from resource/{input filename}
     * @param args
     * @param defaultFileName
     * @param clasz
     * @return
     */
    public static String getPath(String[] args, String defaultFileName, Class clasz) {
        String fileName = getFileName(args, defaultFileName);
        return clasz.getClassLoader().getResource(fileName).getPath();
    }
}
