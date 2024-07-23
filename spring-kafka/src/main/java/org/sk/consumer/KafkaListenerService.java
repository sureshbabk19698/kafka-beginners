package org.sk.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.sk.entity.ConsumerMessageLog;
import org.sk.repository.ConsumerMessageLogRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.MessageHeaders;

import java.util.Date;

@Slf4j
public abstract class KafkaListenerService {
    @Autowired
    private ConsumerMessageLogRepository consumerMessageLogRepository;

    protected final ObjectMapper objectMapper = new ObjectMapper();

    protected void consume(String message, int partition, MessageHeaders messageHeaders) {
        log.info("Inside topic: {} , GroupId : {} , Partition Id: {}", messageHeaders.get(KafkaHeaders.RECEIVED_TOPIC), messageHeaders.get(KafkaHeaders.GROUP_ID), messageHeaders.get(KafkaHeaders.RECEIVED_PARTITION));
        String status = "Success";
        try {
            process(message);
        } catch (Exception e) {
            status = "Failure";
            throw new RuntimeException(e.getMessage());
        } finally {
            updateConsumerMessageLog(message, status, messageHeaders);
            log.info("Processed topic {} & Partition Id {}", messageHeaders.get(KafkaHeaders.RECEIVED_TOPIC), messageHeaders.get(KafkaHeaders.RECEIVED_PARTITION));
        }
    }

    protected abstract void process(String message);

    protected void updateConsumerMessageLog(String payload, String status, MessageHeaders messageHeaders) {
        ConsumerMessageLog consumerLog = ConsumerMessageLog.builder()
                .correlationId((String) messageHeaders.get(KafkaHeaders.CORRELATION_ID))
                .payload(payload)
                .partitionId((Integer) messageHeaders.get(KafkaHeaders.RECEIVED_PARTITION))
                .topic((String) messageHeaders.get(KafkaHeaders.RECEIVED_TOPIC))
                .processStatus(status)
                .messageReceivedTs(new Date())
                .build();
        consumerMessageLogRepository.save(consumerLog);
    }

}
