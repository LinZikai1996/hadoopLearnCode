package com.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

public class Producer {

    public static void main(String[] args) throws IOException {

        //1 properties
        Properties properties = ProducerProperties.getProperties();

        //2 create producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        //3 read source data from CSV
        List<String> data = ReadSourceData.readData("src\\main\\resources\\events.csv");

        //4 check data and consumer function
        String flag = "N";
        System.out.println("consumer function is readily?");
        Scanner scanner = new Scanner(System.in);
        while (flag.equals("N")){
            flag = scanner.nextLine();
            System.out.println("data size = " + data.size());
        }

        //5 use send function
        for (int i = 1 ; i < data.size() ; i++){
            kafkaProducer.send(new ProducerRecord<String, String>(
                    "test",
                    i+"",
                    data.get(i)),
                    (metadata,exception)->{
                        if (exception==null){
                            System.out.println("success");
                        }else{
                            exception.printStackTrace();
                        }
                    });
        }

        //6 close
        kafkaProducer.close();
    }
}
