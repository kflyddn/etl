package com.example.faina.consumer;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Map;

import static com.example.faina.config.KafkaTopicConfig.CSV_TOPIC;
import static com.example.faina.config.KafkaTopicConfig.JSON_TOPIC;

@Service
public class KafkaConsumer {

    //TODO: move to utils
    private CsvSchema csv = CsvSchema.emptySchema().withHeader();
    private CsvMapper csvMapper = new CsvMapper();

    @Autowired
    private KafkaTemplate kafkaTemplate;

   /* @KafkaListener(topics = XML_TOPIC*//*, groupId = "foo"*//*)
    public void listenXml(String message) {
        //TODO: log4j
        System.out.println("Received message: " + message);
        //TODO: transform from xml to json
        // kafkaTemplate.send(jsonMessage);
    }*/

    @KafkaListener(topics = CSV_TOPIC, groupId = "traiana.group")
    public void listenCsv(ConsumerRecord<?, ?> cr) throws Exception {
        //TODO: log4j
       // System.out.println("Received message: " + cr.value());
        //TODO: transform from csv to json
        try {
            //TODO: move to utils
            MappingIterator<Map<?, ?>> mappingIterator =  csvMapper.reader().forType(Map.class).with(csv).readValues(cr.value().toString());
            //TODO: remove surrounding []
            String jsonMessage = mappingIterator.readAll().toString();
       //     System.out.println("JSON: "+jsonMessage);
            kafkaTemplate.send(JSON_TOPIC, jsonMessage);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}
