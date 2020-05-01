package com.example.faina;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;

import java.io.BufferedReader;

import static com.example.faina.config.KafkaTopicConfig.CSV_TOPIC;
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

		//TODO: expose option to override the file name with program argument
		String header = null;
		try (BufferedReader reader = getReader(CsvClient.class, "input.csv")) {
			String line;
			do {
				line = reader.readLine();

				if (line != null) {
					if (header == null)	{
						header = line;
						continue;
					}
					this.template.send(CSV_TOPIC, header+'\n'+line);
					/*Future<RecordMetadata> res =
							kafkaProducer.send(new ProducerRecord
									(KafkaProcessor.TEST_CONNECTION_TOPIC,
											counter % MainConsumer.NUM_OF_THREADS,
											line,
											line)
							);*/
				}
			} while (line != null);
		}

		//latch.await(60, TimeUnit.SECONDS);
		//logger.info("All sent!!!");
		System.out.println("All sent!!!");
		Thread.sleep(200);
	}

}
