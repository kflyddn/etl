package com.example.faina;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;

import static com.example.faina.config.KafkaTopicConfig.CSV_TOPIC;

@SpringBootApplication
public class CsvClient implements CommandLineRunner {

	public static Logger logger = LoggerFactory.getLogger(CsvClient.class);

	public static void main(String[] args) {
		SpringApplication.run(CsvClient.class, args).close();
	}

	@Autowired
	private KafkaTemplate<String, String> template;

	@Override
	public void run(String... args) throws Exception {
		this.template.send(CSV_TOPIC, "foo1");
		this.template.send(CSV_TOPIC, "foo2");
		this.template.send(CSV_TOPIC, "foo3");
		//latch.await(60, TimeUnit.SECONDS);
		//logger.info("All sent!!!");
		System.out.println("All sent!!!");
		Thread.sleep(200);
	}

}
