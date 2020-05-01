package com.example.faina;

import com.example.faina.config.KafkaTopicConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.example.faina.config.KafkaTopicConfig.*;
import static com.example.faina.utils.MessageUtils.sendMessage;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {KafkaTopicConfig.class})
@DirtiesContext
public class IntegrationTest {

    private static Logger logger = LoggerFactory.getLogger(IntegrationTest.class);

    @ClassRule
    public static EmbeddedKafkaRule embeddedKafka = new EmbeddedKafkaRule(1, true);

    @Value(value = "${kafka.bootstrapAddress}")
    private String bootstrapAddress;

    @Value(value = "${kafka.groupId}")
    private String groupId;


    private KafkaMessageListenerContainer<String, String> container;

    private BlockingQueue<ConsumerRecord<String, String>> consumerRecords;

    @Before
    public void setUp() {
        consumerRecords = new LinkedBlockingQueue<>();

        ContainerProperties containerProperties = new ContainerProperties(JSON_TOPIC);

        Map<String, Object> consumerProperties = KafkaTestUtils.consumerProps(
                groupId, "true", embeddedKafka.getEmbeddedKafka());

        DefaultKafkaConsumerFactory<String, String> consumer = new DefaultKafkaConsumerFactory<>(consumerProperties);

        container = new KafkaMessageListenerContainer<>(consumer, containerProperties);
        container.setupMessageListener((MessageListener<String, String>) record -> {
            logger.debug("Listened message='{}'", record.toString());
            consumerRecords.add(record);
        });
        container.start();

        ContainerTestUtils.waitForAssignment(container, embeddedKafka.getEmbeddedKafka().getPartitionsPerTopic());
    }

    @After
    public void tearDown() {
        container.stop();
    }


    @Test
    public void csvFlowTest() throws Exception {

     //   container.setBeanName("templateTests");
        Map<String, Object> producerProps =
                KafkaTestUtils.producerProps(embeddedKafka.getEmbeddedKafka());
        producerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapAddress);
        ProducerFactory<String, String> pf =
                new DefaultKafkaProducerFactory<String, String>(producerProps);
       KafkaTemplate<String, String> template = new KafkaTemplate<>(pf);
        //TODO: assert that records is empty before sending
       // assertThat(records.poll(10, TimeUnit.SECONDS), hasValue("foo"));
        //TODO: fix data
        sendMessage(CSV_TOPIC, "bar", template, logger);
        ConsumerRecord<String, String> received = consumerRecords.poll(10, TimeUnit.SECONDS);
        assertTrue(received != null);
       /* assertThat(received, hasKey(2));
        assertThat(received, hasPartition(0));
        assertThat(received, hasValue("bar"));
        template.send(TEMPLATE_TOPIC, 0, 2, "baz");
        received = records.poll(10, TimeUnit.SECONDS);
        assertThat(received, hasKey(2));
        assertThat(received, hasPartition(0));
        assertThat(received, hasValue("baz"));*/
    }

       /* @Test
    public void csvTestValid() {
        String csvMessage = "header1,header2\nval1,val2";
        sendMessage(CSV_TOPIC, csvMessage, template, logger);
        //TODO: assert
    }

    @Test
    public void csvTestNotValid() {
        String csvMessage = "header1\nval1,val2";
        sendMessage(CSV_TOPIC, csvMessage, template, logger);
        //TODO: assert
    }
*/

}
