package org.sk.springkafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.sk.config.KafkaTopic;
import org.sk.springkafka.entity.ConsumerMessageLog;
import org.sk.springkafka.model.Customer;
import org.sk.springkafka.repository.ConsumerMessageLogRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.Map;
import java.util.Objects;

@Component
@Slf4j
public class KafkaConsumerService {

    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private ConsumerMessageLogRepository consumerMessageLogRepository;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(topics = KafkaTopic.MULTI_PARTITION_TOPIC, groupId = "MG-1")
    public void allPartitionMultiTopicListener(@Payload String message,
                                               @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                               @Headers MessageHeaders messageHeaders) {
        //  MG-1: partitions assigned: [multi_partition_topic-0, multi_partition_topic-1, multi_partition_topic-2]
        log.info("Inside allPartitionMultiTopicListener: Message {} & partition id {}", message, partition);
        updateConsumerMessageLog(message, messageHeaders);
    }


    @KafkaListener(topicPartitions = @TopicPartition(topic = KafkaTopic.MULTI_PARTITION_TOPIC, partitions = {"0, 1"}), groupId = "MG-2")
    public void partitionZeroAndOneMultiTopicListener(@Payload String message,
                                                      @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                                      @Headers MessageHeaders messageHeaders) {
        log.info("Inside partitionZeroAndOneMultiTopicListener: Message {} & partition id {}", message, partition);
        updateConsumerMessageLog(message, messageHeaders);
    }

    @KafkaListener(topicPartitions = @TopicPartition(topic = KafkaTopic.MULTI_PARTITION_TOPIC, partitions = {"2"}),
            groupId = "MG-2")
    public void partitionTwoMultiTopicListener(@Payload String message,
                                               @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                               @Headers MessageHeaders messageHeaders) {
        log.info("Inside partitionTwoMultiTopicListener: Message {} & partition id {}", message, partition);
        updateConsumerMessageLog(message, messageHeaders);
    }

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
        updateConsumerMessageLog(message, messageHeaders);
    }

    @KafkaListener(topics = KafkaTopic.SINGLE_FILTER_PARTITION_TOPIC, groupId = "FG-1", containerFactory = "filterKafkaListenerContainerFactory")
    public void filterSingleTopicListener(@Payload String message,
                                          @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                          @Headers MessageHeaders messageHeaders) {
        log.info("Inside filterSingleTopicListener: Message {} & Partition Id {}", message, partition);
        for (Map.Entry<String, Object> entry : messageHeaders.entrySet()) {
            log.info("Headers : Key {} , Value {}", entry.getKey(), entry.getValue());
        }
        updateConsumerMessageLog(message, messageHeaders);
    }

    @KafkaListener(topics = KafkaTopic.SOURCE_DLT_TOPIC, groupId = "SDLT-1", containerFactory = "sourceDltTopicContainerFactory")
    public void sourceDLTListener(@Payload String message,
                                  @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                  @Headers MessageHeaders messageHeaders) {
        log.info("Inside sourceDLTListener: Message {} & Partition Id {}", message, partition);
        Customer customer = null;
        try {
            customer = objectMapper.readValue(message, Customer.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Json Parser exception.");
        }
        if (Objects.isNull(customer.getCity()) || customer.getCity().isEmpty()) {
            throw new RuntimeException("Not a valid city.");
        }
        updateConsumerMessageLog(message, messageHeaders);
    }

    @KafkaListener(topics = KafkaTopic.RECOVER_DLT_TOPIC, groupId = "RDLT-1", containerFactory = "deadLetterTopicContainerFactory")
    public void dltRecoverListener(@Payload String message,
                                   @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                   @Headers MessageHeaders messageHeaders) {
        log.info("Inside dltRecoverListener: Message {} & Partition Id {}", message, partition);
        Customer customer = null;
        try {
            customer = objectMapper.readValue(message, Customer.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Json Parser exception.");
        }
        if (Objects.isNull(customer.getCity()) || customer.getCity().isEmpty()) {
            throw new RuntimeException("Not a valid city.");
        }
        updateConsumerMessageLog(message, messageHeaders);
    }

    private void updateConsumerMessageLog(String payload, MessageHeaders messageHeaders) {
        ConsumerMessageLog consumerLog = ConsumerMessageLog.builder()
                .correlationId((String) messageHeaders.get(KafkaHeaders.CORRELATION_ID))
                .payload(payload)
                .partitionId((Integer) messageHeaders.get(KafkaHeaders.RECEIVED_PARTITION))
                .topic((String) messageHeaders.get(KafkaHeaders.RECEIVED_TOPIC))
                .processStatus("Success")
                .messageReceivedTs(new Date())
                .build();
        consumerMessageLogRepository.save(consumerLog);
    }

}
