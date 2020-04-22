package com.hadoop.mapreduce.wordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;


import java.io.IOException;

//input LongWritable : 偏移量, Test : value
//output Test, IntWritable

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text word = new Text();
    private final IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //get data
        String line = value.toString();

        //split data
        String[] words = line.split(" ");

        //to map format (word,1) and out
        for (String word: words){
            this.word.set(word);
            context.write(this.word, this.one);
        }
    }
}
