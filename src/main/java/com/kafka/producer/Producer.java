package com.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer {

    public static void main(String[] args) {

        //1 properties

        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);

        //2 create producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        //3 use send function

        for (int i = 0 ; i < 1000 ; i++){
            kafkaProducer.send(new ProducerRecord<String, String>("test",i+"", "message-" + i), (metadata,exception)->{
                if (exception==null){
                    System.out.println("success");
                }else{
                    exception.printStackTrace();
                }
            });
        }

        //4 close
        kafkaProducer.close();
    }
}
