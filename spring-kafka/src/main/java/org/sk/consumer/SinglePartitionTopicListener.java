package org.sk.consumer;

import lombok.extern.slf4j.Slf4j;
import org.sk.config.KafkaTopic;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@Slf4j
public class SinglePartitionTopicListener extends KafkaListenerService {

    @KafkaListener(topics = KafkaTopic.SINGLE_PARTITION_TOPIC, groupId = "SG-1", containerFactory = "defaultKafkaListenerContainerFactory")
    public void singleTopicListener(@Payload String message,
                                    @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                    @Headers MessageHeaders messageHeaders,
                                    Acknowledgment acknowledgment) {
        log.info("Inside singleTopicListener: Message {} & Partition Id {}", message, partition);
        for (Map.Entry<String, Object> entry : messageHeaders.entrySet()) {
            log.info("Headers : Key {} , Value {}", entry.getKey(), entry.getValue());
        }
        acknowledgment.acknowledge();
        consume(message, partition, messageHeaders);
    }

    @KafkaListener(topics = KafkaTopic.SINGLE_FILTER_PARTITION_TOPIC, groupId = "FG-1", containerFactory = "filterKafkaListenerContainerFactory")
    public void filterSingleTopicListener(@Payload String message,
                                          @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                          @Headers MessageHeaders messageHeaders) {
        log.info("Inside filterSingleTopicListener: Message {} & Partition Id {}", message, partition);
        for (Map.Entry<String, Object> entry : messageHeaders.entrySet()) {
            log.info("Headers : Key {} , Value {}", entry.getKey(), entry.getValue());
        }
        consume(message, partition, messageHeaders);
    }
    @Override
    protected void process(String message) {
        // do Nothing
    }

}
