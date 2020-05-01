package com.example.faina.consumer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import static com.example.faina.config.KafkaTopicConfig.CSV_TOPIC;

@Service
public class KafkaConsumer {

    @Autowired
    private KafkaTemplate kafkaTemplate;

   /* @KafkaListener(topics = XML_TOPIC*//*, groupId = "foo"*//*)
    public void listenXml(String message) {
        //TODO: log4j
        System.out.println("Received message: " + message);
        //TODO: transform from xml to json
        // kafkaTemplate.send(jsonMessage);
    }*/

  /*  @KafkaListener(topics = CSV_TOPIC*//*, groupId = "foo"*//*)
    public void listenCsv(String message) {
        //TODO: log4j
        System.out.println("Received message: " + message);
        //TODO: transform from csv to json
       // kafkaTemplate.send(jsonMessage);
    }*/
}
