package com.example.faina.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.ClassRule;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.example.faina.config.KafkaTopicConfig.*;

@Configuration
public class TestConfig {

    private static final String [] topics  = {XML_TOPIC, CSV_TOPIC, ERROR_TOPIC, JSON_TOPIC};

    @Value(value = "${kafka.bootstrapAddress}")
    private String bootstrapAddress;


    @Value(value = "${kafka.groupId}")
    private String groupId;

    @ClassRule
    public static EmbeddedKafkaRule embeddedKafka = new EmbeddedKafkaRule(1, true, topics);

    @Bean
    public EmbeddedKafkaBroker kafkaBroker() {
        //set port
        embeddedKafka.kafkaPorts(9092);

        return embeddedKafka.getEmbeddedKafka();

    }

    @Bean
    public BlockingQueue<ConsumerRecord<String, String>> consumerRecords()  {
        return new LinkedBlockingQueue<>();
    }

    @Bean
    public KafkaMessageListenerContainer<String, String> embeddedContainer(@Autowired EmbeddedKafkaBroker kafkaBroker,
                                                                           @Autowired BlockingQueue<ConsumerRecord<String, String>> consumerRecords)    {
        ContainerProperties containerProperties = new ContainerProperties(topics);

        Map<String, Object> props = KafkaTestUtils.consumerProps(
                groupId, "false", kafkaBroker);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapAddress);
        props.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);
        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);

        DefaultKafkaConsumerFactory<String, String> consumer = new DefaultKafkaConsumerFactory<>(props);

        KafkaMessageListenerContainer<String, String> container =  new KafkaMessageListenerContainer<>(consumer, containerProperties);

        container.setupMessageListener((MessageListener<String, String>) record -> {
            System.out.println("test listener got message:\n"+record.toString());
            consumerRecords.add(record);
        });

        return container;
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate (@Autowired EmbeddedKafkaBroker kafkaBroker)    {
        Map<String, Object> producerProps =
                KafkaTestUtils.producerProps(kafkaBroker);
        producerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapAddress);
        ProducerFactory<String, String> pf =
                new DefaultKafkaProducerFactory<String, String>(producerProps);
        return new KafkaTemplate<>(pf);
    }
}
