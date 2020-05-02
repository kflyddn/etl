package com.example.faina.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

import static com.example.faina.config.KafkaTopicConfig.XML_TOPIC;
import static com.example.faina.utils.InputUtils.getFileName;
import static com.example.faina.utils.MessageUtils.sendMessage;

@SpringBootApplication
@Profile("xml")
public class XmlClient implements CommandLineRunner {

	private static Logger logger = LoggerFactory.getLogger(XmlClient.class);

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Override
	public void run(String... args) throws Exception {

		String fileName = getFileName(args, "input.xml");
		String pathStr = XmlClient.class.getClassLoader().getResource(fileName).getPath();
		StringBuilder sb = new StringBuilder();

		try (Stream<String> stream = Files.lines( Paths.get(pathStr), StandardCharsets.UTF_8)) {
			stream.forEach(s -> sb.append(s).append("\n"));
			if (sb.toString().isBlank())	{
				return;
			}
			String message = sb.toString();
			sendMessage(XML_TOPIC, message, this.kafkaTemplate, logger);
		}

		Thread.sleep(20000);
	}

}
