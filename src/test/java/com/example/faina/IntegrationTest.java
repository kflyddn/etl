package com.example.faina;

import com.example.faina.config.TestConfig;
import com.example.faina.consumer.TraianaKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.After;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.example.faina.config.KafkaTopicConfig.*;
import static com.example.faina.utils.MessageUtils.sendMessage;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {TestConfig.class, TraianaKafkaConsumer.class})
@DirtiesContext
public class IntegrationTest {

    private static Logger logger = LoggerFactory.getLogger(IntegrationTest.class);

    @Autowired
    private EmbeddedKafkaBroker kafkaBroker;

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
        ContainerTestUtils.waitForAssignment(container, 8);
    }

    @After
    public void tearDown() {
        container.stop();
    }


    @Test
    public void csvFlowTest() throws InterruptedException {

        assertTrue(consumerRecords.isEmpty());
        String csvMessage = "id,header1, header2\n1,val1,val2";
      //  Thread.sleep(10000);
        sendMessage(CSV_TOPIC, csvMessage, kafkaTemplate, logger);
      //  Thread.sleep(10000);
        ConsumerRecord<String, String> received = consumerRecords.poll(10, TimeUnit.SECONDS);
        assertTrue(received != null);
        //TODO: fix expected value
        String expected = "";
        assertTrue(received.topic().equals(CSV_TOPIC));
        assertTrue(received.value().equals(expected));
    }

    @Test
    public void csvNotValidTest() throws InterruptedException {
        assertTrue(consumerRecords.isEmpty());
        String csvInvalid = "id,header1, header2\nval1,val2";
        sendMessage(CSV_TOPIC, csvInvalid, kafkaTemplate, logger);
        ConsumerRecord<String, String> received = consumerRecords.poll(10, TimeUnit.SECONDS);
        assertTrue(received != null);
        //assert that consumerRecords contains error message
        assertTrue(received.topic().equals(ERROR_TOPIC));
    }

}
