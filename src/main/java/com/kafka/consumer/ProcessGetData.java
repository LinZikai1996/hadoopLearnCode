package com.kafka.consumer;


import org.apache.hadoop.io.Writable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ProcessGetData {

    public static String toFile(List<String> dataList , int index) throws IOException {

        List<String> resultList = processData(dataList);
        return WriteData.write(resultList, "events", String.valueOf(index));
    }

    private static String changeTimestamp(String s){
        long timestamp = Long.parseLong(s);
        return new java.text.SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(new java.util.Date(timestamp));
    }

    private static List<String> processData(List<String> dataList){
        List<String> results = new ArrayList<>();
        StringBuilder stringBuilder = new StringBuilder();

        for (String data : dataList) {
            String[] strings = data.split(",");
            String date = changeTimestamp(strings[0]);


            stringBuilder.append(date).append(",");
            for (int i = 1; i < strings.length; i++) {
                stringBuilder.append(strings[i]).append(",");
            }
            results.add(stringBuilder.subSequence(0, stringBuilder.length() - 1).toString());
            stringBuilder.delete(0, stringBuilder.length());
        }
        return results;
    }
}
