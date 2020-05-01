package com.example.faina.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;

public class InputUtils {

    public static BufferedReader getReader(Class clasz, String fileName) throws FileNotFoundException {
        File file = new File(
                clasz.getClassLoader().getResource(fileName).getFile()
        );
        return new BufferedReader(new FileReader(file));
    }
}
