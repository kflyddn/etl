package com.example.faina;

import com.example.faina.config.KafkaConsumerConfig;
import com.example.faina.config.KafkaProducerConfig;
import com.example.faina.config.KafkaTopicConfig;
import com.example.faina.config.TestConfig;
import com.example.faina.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.After;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.junit4.SpringRunner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.example.faina.config.KafkaTopicConfig.*;
import static com.example.faina.utils.MessageUtils.sendMessage;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {
        TestConfig.class,
        KafkaConsumerConfig.class,
        KafkaProducerConfig.class,
        KafkaTopicConfig.class,
        KafkaConsumer.class
})
public class IntegrationTest {

    private static final long WAIT  = 3000L;

    private static Logger logger = LoggerFactory.getLogger(IntegrationTest.class);

    private static final String [] topicsToSubscribe = {ERROR_TOPIC, JSON_TOPIC};

    @Autowired
    private KafkaMessageListenerContainer<String, String> container;

    @Autowired
    private  KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private BlockingQueue<ConsumerRecord<String, String>> consumerRecords;

    @BeforeEach
    public void setUp() {
        consumerRecords.clear();
        container.start();
        ContainerTestUtils.waitForAssignment(container, 2);
    }

    @After
    public void tearDown() {
        container.stop();
    }


    @Test
    public void csvToJsonTest() throws InterruptedException {

        String csvMessage = "id,header1, header2\n1,val1,val2";
        sendMessage(CSV_TOPIC, csvMessage, kafkaTemplate, logger);
        Thread.sleep(WAIT);
        ConsumerRecord<String, String> received = consumerRecords.poll(10, TimeUnit.SECONDS);
        assertTrue(received != null);
        String expected = "{\"header2\":\"val2\",\"id\":\"1\",\"header1\":\"val1\"}";
        assertTrue(received.topic().equals(JSON_TOPIC));
        assertTrue(received.value().equals(expected));
    }

    @Test
    public void csvNotValidTest() throws InterruptedException {
        String csvInvalid = "\"";
        sendMessage(CSV_TOPIC, csvInvalid, kafkaTemplate, logger);
        Thread.sleep(WAIT);
        ConsumerRecord<String, String> received = consumerRecords.poll(10, TimeUnit.SECONDS);
        assertTrue(received != null);
        //assert that consumerRecords contains error message
        assertTrue(received.topic().equals(ERROR_TOPIC));
    }

    @Test
    public void xmlToJsonTest() throws InterruptedException {

        String xmlMessage = "<message id=\"1\">\n" +
                "<field1>val1</field1>\n" +
                "<field2>val2</field2>\n" +
                "</message>";
        sendMessage(XML_TOPIC, xmlMessage, kafkaTemplate, logger);
        Thread.sleep(WAIT);
        ConsumerRecord<String, String> received = consumerRecords.poll(10, TimeUnit.SECONDS);
        assertTrue(received != null);
        String expected = "{\"id\":\"1\",\"field1\":\"val1\",\"field2\":\"val2\"}";
        assertTrue(received.topic().equals(JSON_TOPIC));
        assertTrue(received.value().equals(expected));
    }

    @Test
    public void xmlNotValidTest() throws InterruptedException {
        String xmlInvalid = "<message id=";
        sendMessage(XML_TOPIC, xmlInvalid, kafkaTemplate, logger);
        Thread.sleep(WAIT);
        ConsumerRecord<String, String> received = consumerRecords.poll(10, TimeUnit.SECONDS);
        assertTrue(received != null);
        //assert that consumerRecords contains error message
        assertTrue(received.topic().equals(ERROR_TOPIC));
    }

}
