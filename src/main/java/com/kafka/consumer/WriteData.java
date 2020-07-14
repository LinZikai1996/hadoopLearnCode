package com.kafka.consumer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class WriteData {

    public static String write(List<String> data, String fileName, String index) throws IOException {
        String file = "D:\\data\\getData\\" + fileName + "_" + index + ".csv";
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file));

        for (String s:data){
            bufferedWriter.write(s + "\n");
        }
        return fileName + "_" + index + ".csv";
    }
}
