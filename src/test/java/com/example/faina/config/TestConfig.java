package com.example.faina.config;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.example.faina.config.KafkaTopicConfig.*;

@Configuration
public class TestConfig {

    private static final String [] topicsToSubscribe = {ERROR_TOPIC, JSON_TOPIC};

    @Bean
    public BlockingQueue<ConsumerRecord<String, String>> consumerRecords()  {
        return new LinkedBlockingQueue<>();
    }

    @Bean
    public KafkaMessageListenerContainer<String, String> container(
            @Autowired ConsumerFactory<String, String> consumer,
            @Autowired BlockingQueue<ConsumerRecord<String, String>> consumerRecords)    {
        ContainerProperties containerProperties = new ContainerProperties(topicsToSubscribe);

        KafkaMessageListenerContainer<String, String> container =  new KafkaMessageListenerContainer<>(consumer, containerProperties);

        container.setupMessageListener((MessageListener<String, String>) record -> {
            System.out.println("test listener got message:\n"+record.value());
            consumerRecords.add(record);
        });

        return container;
    }

}
