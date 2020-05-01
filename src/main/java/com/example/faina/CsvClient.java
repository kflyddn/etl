package com.example.faina;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.io.BufferedReader;

import static com.example.faina.config.KafkaTopicConfig.*;
import static com.example.faina.utils.InputUtils.getReader;

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

		String fileName = getFileName(args);

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
					sendMessage(CSV_TOPIC, header+'\n'+line);
				}
			} while (line != null);
		}

		Thread.sleep(200);
	}

	public void sendMessage(String topic, String message) {

		ListenableFuture<SendResult<String, String>> future =
				this.template.send(topic, message);

		future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {

			@Override
			public void onSuccess(SendResult<String, String> result) {
				logger.info("Sent message=[" + message +
						"] with offset=[" + result.getRecordMetadata().offset() + "]");
			}
			@Override
			public void onFailure(Throwable ex) {
				logger.error("Unable to send message=["
						+ message + "] due to : " + ex.getMessage());
				//TODO: fix error message format, extract formatting method to utils
				template.send(ERROR_TOPIC, message);
			}
		});
	}

	private String getFileName(String[] args) {
		String fileName = "input.csv";
		//override the file name with program argument
		if (args != null && args.length > 0 && args[0] != null)	{
			fileName = args[0];
		}
		return fileName;
	}

}
