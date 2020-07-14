package com.kafka.producer;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class ReadSourceData {

    public static List<String> readData(String path) throws IOException {

        List<String> data = new ArrayList<>();

        File file = new File(path);
        String line = "";
        if (file.exists()){
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            while ( (line =bufferedReader.readLine()) != null){
                data.add(line);
            }
        } else {
            System.out.println("file is not exist");
        }

        return data;
    }
}
