package com.example.faina;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;

import static com.example.faina.config.KafkaTopicConfig.CSV_TOPIC;
import static com.example.faina.config.KafkaTopicConfig.XML_TOPIC;
/*

@SpringBootApplication
public class TraianaApplication {

	public static void main(String[] args) {
		SpringApplication.run(TraianaApplication.class, args);
	}

}
*/

@SpringBootApplication
public class TraianaApplication implements CommandLineRunner {

	public static Logger logger = LoggerFactory.getLogger(TraianaApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(TraianaApplication.class, args).close();
	}

	@Autowired
	private KafkaTemplate<String, String> template;

//	private final CountDownLatch latch = new CountDownLatch(3);

	@Override
	public void run(String... args) throws Exception {
		this.template.send(CSV_TOPIC, "foo1");
		this.template.send(CSV_TOPIC, "foo2");
		this.template.send(CSV_TOPIC, "foo3");
		//latch.await(60, TimeUnit.SECONDS);
		//logger.info("All sent!!!");
		System.out.println("All sent!!!");
	}

	@KafkaListener(topics = CSV_TOPIC)
	public void listen(ConsumerRecord<?, ?> cr) throws Exception {
		System.out.println("Received message: " + cr);
	//	logger.info(cr.toString());
	//	latch.countDown();
	}

}
