package com.example.faina.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;

import static com.example.faina.config.KafkaTopicConfig.*;
import static com.example.faina.utils.CsvUtils.csvToJson;
import static com.example.faina.utils.MessageUtils.sendMessage;

@Service
public class KafkaConsumer {

    private static Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @KafkaListener(topics = XML_TOPIC)
    public void listenXml(ConsumerRecord<?, ?> cr)  {
        XmlMapper xmlMapper = new XmlMapper();
        JsonNode node = null;
        try {
            node = xmlMapper.readTree(cr.value().toString().getBytes());
            ObjectMapper jsonMapper = new ObjectMapper();
            String json = jsonMapper.writeValueAsString(node);
            sendMessage(JSON_TOPIC, json, kafkaTemplate, logger);
        } catch (IOException e) {
            String errMessage = "this message is not valid xml: \n"+cr;
            kafkaTemplate.send(ERROR_TOPIC, errMessage);
        }

    }

    @KafkaListener(topics = CSV_TOPIC)
    public void listenCsv(ConsumerRecord<?, ?> cr) throws Exception {
       logger.info("Received csv message: " + cr.value());
        try {
            String jsonMessage = csvToJson(cr.value().toString());
            sendMessage(JSON_TOPIC, jsonMessage, kafkaTemplate, logger);
        } catch(Exception e) {
            String errMessage = "this message is not valid csv: \n"+cr;
            kafkaTemplate.send(ERROR_TOPIC, errMessage);
        }
    }

    @KafkaListener(topics = JSON_TOPIC)
    public void listenJson(ConsumerRecord<?, ?> cr) {
        logger.info("Received json message:\n"+cr.value());
        //TODO: send json via REST
    }

    @KafkaListener(topics = ERROR_TOPIC)
    public void listenError(ConsumerRecord<?, ?> cr) throws Exception {
        logger.error("Received error message:\n"+cr.value());
        //TODO: write to database

    }
}
