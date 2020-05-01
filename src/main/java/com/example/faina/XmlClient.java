package com.example.faina;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;

import static com.example.faina.utils.InputUtils.getFileName;

@SpringBootApplication
public class XmlClient implements CommandLineRunner {

	private static Logger logger = LoggerFactory.getLogger(XmlClient.class);

	public static void main(String[] args) {

		SpringApplication.run(XmlClient.class, args).close();
	}

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Override
	public void run(String... args) throws Exception {

		String fileName = getFileName(args, "input.xml");

		//TODO: read all from file, send to kafka

		Thread.sleep(20000);
	}

}
