package com.example.faina.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import static com.example.faina.config.KafkaTopicConfig.*;
import static com.example.faina.utils.CsvUtils.csvToJson;
import static com.example.faina.utils.MessageUtils.sendMessage;

@Service
public class KafkaConsumer {

    private static Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @KafkaListener(topics = XML_TOPIC)
    public void listenXml(ConsumerRecord<?, ?> cr) {
       //TODO
    }

    @KafkaListener(topics = CSV_TOPIC)
    public void listenCsv(ConsumerRecord<?, ?> cr) throws Exception {
       logger.info("Received csv message: " + cr.value());
        try {
            String jsonMessage = csvToJson(cr.value().toString());
            sendMessage(JSON_TOPIC, jsonMessage, kafkaTemplate, logger);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    @KafkaListener(topics = JSON_TOPIC)
    public void listenJson(ConsumerRecord<?, ?> cr) {
        logger.info("Received json message:\n"+cr.value());
    }

    @KafkaListener(topics = ERROR_TOPIC)
    public void listenError(ConsumerRecord<?, ?> cr) throws Exception {
        logger.info("Received error message:\n"+cr.value());
        //TODO: write to database

    }
}
