package com.kafka.consumer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

public class PutFileToHdfs {


    public static void put(String fileName, int index) throws IOException, InterruptedException {
        FileSystem fileSystem = FileSystem.get(
                URI.create("hdfs://hadoop100:9000"),
                new Configuration(),
                "hadoop");

        boolean result =  fileSystem.delete(new Path("/hadoopHomeWork/events/part=" + index +"/*"),true);
        if (result){
            System.out.println("delete is ok");
        } else {
            System.out.println("failed");
        }

        fileSystem.copyFromLocalFile(new Path("D:\\data\\getData\\" + fileName),
                new Path("/hadoopHomeWork/events/part=" + index +"/"+ fileName));
    }
}
