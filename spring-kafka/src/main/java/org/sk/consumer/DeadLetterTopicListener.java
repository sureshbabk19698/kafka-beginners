package org.sk.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.sk.config.KafkaTopic;
import org.sk.model.Customer;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.Objects;

@Component
@Slf4j
public class DeadLetterTopicListener extends KafkaListenerService {

    @KafkaListener(topics = KafkaTopic.SOURCE_DLT_TOPIC, groupId = "SDLT-1", containerFactory = "sourceDltTopicContainerFactory")
    public void sourceDLTListener(@Payload String message,
                                  @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                  @Headers MessageHeaders messageHeaders) {
        consume(message, partition, messageHeaders);
    }

    @KafkaListener(topics = KafkaTopic.RECOVER_DLT_TOPIC, groupId = "RDLT-1", containerFactory = "deadLetterTopicContainerFactory")
    public void dltRecoverListener(@Payload String message,
                                   @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                   @Headers MessageHeaders messageHeaders) {
        consume(message, partition, messageHeaders);
    }

    @Override
    protected void process(String message) {
        Customer customer = null;
        try {
            customer = objectMapper.readValue(message, Customer.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Json Parser exception.");
        }
        if (Objects.isNull(customer.getCity()) || customer.getCity().isEmpty()) {
            throw new RuntimeException("Not a valid city.");
        }
    }

}
