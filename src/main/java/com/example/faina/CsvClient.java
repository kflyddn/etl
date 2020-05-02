package com.example.faina;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;

import java.io.BufferedReader;

import static com.example.faina.config.KafkaTopicConfig.*;
import static com.example.faina.utils.InputUtils.getFileName;
import static com.example.faina.utils.InputUtils.getReader;
import static com.example.faina.utils.MessageUtils.sendMessage;

@SpringBootApplication
public class CsvClient implements CommandLineRunner {

	private static Logger logger = LoggerFactory.getLogger(CsvClient.class);

	public static void main(String[] args) {

		SpringApplication.run(CsvClient.class, args).close();
	}

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Override
	public void run(String... args) throws Exception {

		String fileName = getFileName(args, "input.csv");

		String header = null;
		try (BufferedReader reader = getReader(CsvClient.class, fileName)) {
			String line;
			do {
				line = reader.readLine();
				if (line != null) {
					if (header == null)	{
						header = line;
						continue;
					}
					String message = header+'\n'+line;
					sendMessage(CSV_TOPIC, message, this.kafkaTemplate, logger);
				}
			} while (line != null);
		}

		Thread.sleep(20000);
	}

}
