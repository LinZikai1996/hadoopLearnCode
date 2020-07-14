package com.hadoop.hdfsClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

public class hdfsClient {

    private FileSystem fileSystem;

    @Before
    public void before() throws IOException, InterruptedException {
        System.out.println("before");
        //获取一个HDFS的抽象封装对象
        fileSystem = FileSystem.get(
                URI.create("hdfs://hadoop100:9000"),
                new Configuration(),
                "hadoop");

    }

    @Test
    public void put() throws IOException, InterruptedException {

        //用这个对象操作文件系统
        fileSystem.copyFromLocalFile(new Path("C:\\Users\\Zikai\\Documents\\Tencent Files\\835163838\\FileRecv\\tianchi_mobile_recommend_train_user.csv"),new Path("/landing/event/part_date=2020-06-06/"));

        //关闭文件系统
        fileSystem.close();
    }

    @Test
    public void get() throws IOException, InterruptedException {
        fileSystem.copyToLocalFile(new Path("/2.txt"),new Path("src/main/resources/"));
    }

    @Test
    public void rename() throws IOException, InterruptedException {
        fileSystem.rename(new Path("/1.txt"),new Path("/3.txt"));
    }

    @Test
    public void delete() throws IOException {
        boolean result =  fileSystem.delete(new Path("/landing/event/part_date=2020-06-06/tianchi_mobile_recommend_train_user.csv"),true);
        if (result){
            System.out.println("delete is ok");
        } else {
            System.out.println("failed");
        }

    }

    @Test
    public void append() throws IOException {
        FSDataOutputStream fsDataOutputStream = fileSystem.append(new Path("/1.txt"),1024);

        FileInputStream fileInputStream = new FileInputStream("src/main/resources/append.txt");

        IOUtils.copyBytes(fileInputStream,fsDataOutputStream,1024,true);
    }

    @Test
    public void fileStatuses() throws IOException {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));
        for (FileStatus fileStatus:fileStatuses){
            if (fileStatus.isFile()){
                System.out.println(fileStatus.getPath() + " is file");
                System.out.println(fileStatus.getLen());
            } else {
                System.out.println(fileStatus.getPath() + "is dir");
            }
        }
    }

    @Test
    public void listFiles() throws IOException {
        RemoteIterator<LocatedFileStatus> fileStatusRemoteIterator =
                fileSystem.listFiles(new Path("/"),true);
        while (fileStatusRemoteIterator.hasNext()){
            LocatedFileStatus fileStatus = fileStatusRemoteIterator.next();

            System.out.println("file path" + fileStatus.getPath());
            BlockLocation[] blockLocations =fileStatus.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations){
                String[] hosts = blockLocation.getHosts();
                System.out.println(" block at ");
                for (String host:hosts){
                    System.out.println(host + " " );
                }
            }
        }
    }

    @Test
    public void readList() throws IOException{

    }

    @After
    public void after() throws IOException {
        System.out.println("after");
        fileSystem.close();
    }


}
