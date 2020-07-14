package com.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.util.*;

public class Consumer {

    public static void main(String[] args) throws IOException, InterruptedException {

        //1 properties
        Properties properties = new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());


        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "hadoop");

        //2 create consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        kafkaConsumer.subscribe(Arrays.asList("test"));

        //3 poll
        int index = 0;
        List<String> dataList = new ArrayList<>();
        boolean flagExit = false;
        while (true){
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(1000);


            for (ConsumerRecord<String,String> consumerRecord:consumerRecords){
                if (!"exit".equals(consumerRecord.value())){
                    dataList.add(consumerRecord.value());
                } else {
                    System.out.println(consumerRecord.value());
                    flagExit = true;
                }
            }
            if (dataList.size() >= 500000 || flagExit){
                index++;
                String fileName =  ProcessGetData.toFile(dataList, index);
                System.out.println(fileName);
                PutFileToHdfs.put(fileName, index);
                dataList.clear();
            }
        }
    }
}
