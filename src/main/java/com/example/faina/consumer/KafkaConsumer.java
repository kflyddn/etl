package com.example.faina.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.io.IOException;

import static com.example.faina.config.KafkaTopicConfig.*;
import static com.example.faina.utils.MessageUtils.sendMessage;
import static com.example.faina.utils.TransformUtils.*;

@Service
public class KafkaConsumer {

    private static Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);
    private static final String logMessage = "%s listener received a message:\n%s";

    public KafkaConsumer()  {
        logger.info("creating KafkaConsumer");
    }

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @KafkaListener(topics = XML_TOPIC)
    public void listenXml(ConsumerRecord<?, ?> cr, Acknowledgment ack)  {
        logger.info(String.format(logMessage, "xml", cr.value()));

        try {
            String json = xmlToJsonString(cr);
            sendMessage(JSON_TOPIC, json, kafkaTemplate, logger);
        } catch (IOException e) {
            String errMessage = "this message is not valid xml: \n"+cr;
            kafkaTemplate.send(ERROR_TOPIC, errMessage);
        }finally {
            ack.acknowledge();
        }
    }

    @KafkaListener(topics = CSV_TOPIC)
    public void listenCsv(ConsumerRecord<?, ?> cr, Acknowledgment ack) throws Exception {
       logger.info(String.format(logMessage, "csv", cr.value()));
        try {
            String jsonMessage = csvToJson(cr.value().toString()).toString();
            sendMessage(JSON_TOPIC, jsonMessage, kafkaTemplate, logger);
        } catch(Exception e) {
            String errMessage = "this message is not valid csv: \n"+cr;
            kafkaTemplate.send(ERROR_TOPIC, errMessage);
        }finally {
            ack.acknowledge();
        }
    }

    @KafkaListener(topics = JSON_TOPIC)
    public void listenJson(ConsumerRecord<?, ?> cr, Acknowledgment ack) {
        logger.info(String.format(logMessage, "json", cr.value()));
        ack.acknowledge();
    }

    @KafkaListener(topics = ERROR_TOPIC)
    public void listenError(ConsumerRecord<?, ?> cr, Acknowledgment ack) throws Exception {
        logger.error(String.format(logMessage, "error", cr.value()));
        //TODO: write to database
        ack.acknowledge();

    }
}
