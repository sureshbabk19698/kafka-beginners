package org.sk.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.sk.entity.ConsumerMessageLog;
import org.sk.repository.ConsumerMessageLogRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
@Slf4j
public class KafkaDeadLetterTopicConfig {

    @Autowired
    private ConsumerMessageLogRepository consumerMessageLogRepository;

    private final ObjectMapper objectMapper = new ObjectMapper();


    @Bean("sourceDltTopicContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, String> sourceDltTopicContainerFactory(
            @Qualifier("sourceDltTopicTemplate") KafkaTemplate<String, Object> kafkaTemplate,
            @Qualifier("consumerFactory") ConsumerFactory<String, String> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.setCommonErrorHandler(publishTotDeadLetterTopicErrorHandler(kafkaTemplate));
        return factory;
    }

    private CommonErrorHandler publishTotDeadLetterTopicErrorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
        return new DefaultErrorHandler(new DeadLetterPublishingRecoverer(kafkaTemplate,
                ((consumerRecord, e) -> {
                    log.info("Error occurred {}, publishing data to DLT.", e.getMessage());
                    return new TopicPartition(KafkaTopic.RECOVER_DLT_TOPIC, -1);
                })), new FixedBackOff(0L, 0L));
    }

    @Bean("deadLetterTopicContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, String> deadLetterTopicContainerFactory(
            @Qualifier("consumerFactory") ConsumerFactory<String, String> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(getDeadLetterTopicErrorHandler());
        return factory;
    }

    private CommonErrorHandler getDeadLetterTopicErrorHandler() {
        return new DefaultErrorHandler((consumerRecord, e) -> {
            try {
                log.error("Failed to process data after multiple retries {}", e.getCause().getMessage());
                ConsumerMessageLog consumerLog = ConsumerMessageLog.builder()
                        .correlationId(new String(consumerRecord.headers().lastHeader(KafkaHeaders.CORRELATION_ID).value()))
                        .payload(objectMapper.writeValueAsString(consumerRecord.value()))
                        .partitionId(consumerRecord.partition())
                        .topic(consumerRecord.topic())
                        .errorMessage(e.getCause().getMessage())
                        .processStatus("Failure")
                        .build();
                consumerMessageLogRepository.save(consumerLog);
            } catch (JsonProcessingException ex) {
                throw new RuntimeException(ex);
            }
        }, new FixedBackOff(3000L, 1L));
    }
}
