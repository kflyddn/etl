package com.example.faina.client.impl;

import com.example.faina.client.InputClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;

import java.io.BufferedReader;

import static com.example.faina.config.KafkaTopicConfig.*;
import static com.example.faina.utils.InputUtils.getFileName;
import static com.example.faina.utils.InputUtils.getReader;
import static com.example.faina.utils.MessageUtils.sendMessage;

/**
 * CSV client reads multiple messages from single .csv file
 * the client sends messages one by one, each message comes with the original csv header
 */
@SpringBootApplication
@Profile("csv")
public class CsvClient implements InputClient {

	private static Logger logger = LoggerFactory.getLogger(CsvClient.class);

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
