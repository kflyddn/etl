package com.example.faina.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import static com.example.faina.config.KafkaTopicConfig.CSV_TOPIC;
import static com.example.faina.config.KafkaTopicConfig.JSON_TOPIC;
import static com.example.faina.utils.CsvUtils.csvToJson;

@Service
public class KafkaConsumer {

    private static Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

    @Autowired
    private KafkaTemplate kafkaTemplate;

   /* @KafkaListener(topics = XML_TOPIC*//*, groupId = "foo"*//*)
    public void listenXml(String message) {
       //TODO
    }*/

    @KafkaListener(topics = CSV_TOPIC, groupId = "traiana.group")
    public void listenCsv(ConsumerRecord<?, ?> cr) throws Exception {
       logger.info("Received message: " + cr.value());
        try {
            String jsonMessage = csvToJson(cr.value().toString());
            //TODO: add onSuccess/onFailure
            kafkaTemplate.send(JSON_TOPIC, jsonMessage);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}
