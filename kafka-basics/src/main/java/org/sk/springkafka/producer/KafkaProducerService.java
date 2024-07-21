package org.sk.springkafka.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.sk.springkafka.entity.ProducerMessageLog;
import org.sk.springkafka.model.Customer;
import org.sk.springkafka.repository.ProducerMessageLogRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.Objects;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;

@Component
@Slf4j
public class KafkaProducerService {

    @Autowired
    @Qualifier("singlePartitionTopicTemplate")
    private KafkaTemplate<String, Object> singlePartitionTopicTemplate;

    @Autowired
    @Qualifier("multiPartitionTopicTemplate")
    private KafkaTemplate<String, Object> multiPartitionTopicTemplate;

    @Autowired
    @Qualifier("singleFilterPartitionTopicTemplate")
    private KafkaTemplate<String, Object> singleFilterPartitionTopicTemplate;

    @Autowired
    @Qualifier("sourceDltTopicTemplate")
    private KafkaTemplate<String, Object> sourceDltTopicTemplate;

    @Autowired
    private ProducerMessageLogRepository producerMessageLogRepository;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final Function<Message<String>, BiFunction<SendResult<String, Object>, Throwable, Object>> processResult = msg -> (res, ex) -> {
        String result = "Success";
        RecordMetadata rmd = null;
        ProducerRecord<String, Object> pr = null;
        String errorMessage = null;
        if (res != null) {
            rmd = res.getRecordMetadata();
            pr = res.getProducerRecord();
            log.info("Topic: {}, Sent message: {}, Offset: {} ", rmd.topic(), pr.value(), rmd.offset());
        }
        if (ex != null) {
            result = "Failed";
            errorMessage = ex.getMessage();
            log.info("Unable to send message={} due to : {} ", msg, ex.getMessage());
        }
        ProducerMessageLog producerMessageLog = ProducerMessageLog.builder()
                .correlationId((String) msg.getHeaders().get(KafkaHeaders.CORRELATION_ID))
                .payload(msg.getPayload())
                .partitionId((Integer) msg.getHeaders().get(KafkaHeaders.RECEIVED_PARTITION))
                .topic((String) msg.getHeaders().get(KafkaHeaders.TOPIC))
                .errorMessage(errorMessage)
                .messagesSentTs(new Date())
                .processStatus(result)
                .build();
        producerMessageLogRepository.save(producerMessageLog);
        return result;
    };

    public void publishMessage(Customer customer) {
        singlePartitionTopicPublish(customer);
        multiPartitionTopicPublish(customer);
    }

    public void singlePartitionTopicPublish(Customer customer) {
        try {
            String jsonValue = objectMapper.writeValueAsString(customer);
            Message<String> message = getMessage(jsonValue, null, singlePartitionTopicTemplate);
            singlePartitionTopicTemplate.send(message).handleAsync(processResult.apply(message));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public void singlePartitionTopicWithEventTypePublish(Customer customer) {
        try {
            String jsonValue = objectMapper.writeValueAsString(customer);
            Message<String> message = getMessage(jsonValue, null, singleFilterPartitionTopicTemplate);
            singleFilterPartitionTopicTemplate.send(message).handleAsync(processResult.apply(message));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void multiPartitionTopicPublish(Customer customer) {
        try {
            String jsonValue = objectMapper.writeValueAsString(customer);
            Message<String> partition0 = getMessage(jsonValue, 0, multiPartitionTopicTemplate);
            Message<String> partition1 = getMessage(jsonValue, 1, multiPartitionTopicTemplate);
            Message<String> partition2 = getMessage(jsonValue, 2, multiPartitionTopicTemplate);
            multiPartitionTopicTemplate.send(partition0).handleAsync(processResult.apply(partition0));
            multiPartitionTopicTemplate.send(partition1).handleAsync(processResult.apply(partition1));
            multiPartitionTopicTemplate.send(partition2).handleAsync(processResult.apply(partition2));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void sourceDltTopicTemplatePublish(Customer customer) {
        try {
            String jsonValue = objectMapper.writeValueAsString(customer);
            Message<String> message = getMessage(jsonValue, null, sourceDltTopicTemplate);
            sourceDltTopicTemplate.send(message).handleAsync(processResult.apply(message));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Message<String> getMessage(String jsonValue, Integer partitionId, KafkaTemplate kafkaTemplate) {
        Message<String> message = MessageBuilder.withPayload(jsonValue)
                .setHeader(KafkaHeaders.TOPIC, kafkaTemplate.getDefaultTopic())
                .setHeader(KafkaHeaders.CORRELATION_ID, UUID.randomUUID().toString())
                .setHeader("source", "Spring")
                .build();
        if (Objects.nonNull(partitionId)) {
            message.getHeaders().put(KafkaHeaders.PARTITION, partitionId);
        }
        return message;
    }
}
