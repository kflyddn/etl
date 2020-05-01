package com.example.faina.utils;

import org.slf4j.Logger;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import static com.example.faina.config.KafkaTopicConfig.ERROR_TOPIC;

public class MessageUtils {

    public static void sendMessage(String topic, String message, KafkaTemplate<String, String> template, Logger logger) {

        ListenableFuture<SendResult<String, String>> future =
                template.send(topic, message);

        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {

            @Override
            public void onSuccess(SendResult<String, String> result) {
                String infoMessage = "Sent message=[" + message +
                        "] with offset=[" + result.getRecordMetadata().offset() + "]";
                logger.info(infoMessage);
                //TODO: persist infoMessage to ES
            }
            @Override
            public void onFailure(Throwable ex) {
                String errMessage = "Unable to send message=["
                        + message + "] due to : " + ex.getMessage();
                logger.error(errMessage);
                //TODO: persist errMessage to ES
                template.send(ERROR_TOPIC, errMessage);
            }
        });
    }
}
