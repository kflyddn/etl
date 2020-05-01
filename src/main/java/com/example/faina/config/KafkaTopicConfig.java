package com.example.faina.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopicConfig {

    public static final String XML_TOPIC  = "xml_topic";
    public static final String CSV_TOPIC  = "csv_topic";
    public static final String ERROR_TOPIC = "error_topic";
    public static final String JSON_TOPIC = "json_topic";

    @Bean
    public NewTopic xmlTopic() {
        return new NewTopic(XML_TOPIC, 1, (short) 1);
    }

    @Bean
    public NewTopic csvTopic() {
        return new NewTopic(CSV_TOPIC, 1, (short) 1);
    }

    @Bean
    public NewTopic jsonTopic() {
        return new NewTopic(JSON_TOPIC, 1, (short) 1);
    }

    @Bean
    public NewTopic errorTopic() {
        return new NewTopic(ERROR_TOPIC, 1, (short) 1);
    }
}
