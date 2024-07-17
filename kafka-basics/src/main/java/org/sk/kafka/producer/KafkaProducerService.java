package org.sk.kafka.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.function.BiFunction;
import java.util.function.Function;

@Component
@Slf4j
public class KafkaProducerService {

    @Autowired
    @Qualifier("singlePartitionTopicKafkaTemplate")
    private KafkaTemplate<String, Object> singlePartitionTopicKafkaTemplate;

    @Autowired
    @Qualifier("multiPartitionTopicKafkaTemplate")
    private KafkaTemplate<String, Object> multiPartitionTopicKafkaTemplate;

    public void sendMessage() {
        Message<String> message = MessageBuilder.withPayload("Publishing data")
                .setHeader("source", "Spring")
                .build();
        Function<Message<String>, BiFunction<SendResult<String, Object>, Throwable, Object>> processResult = msg -> (res, ex) -> {
            String result = "SUCCESS";
            if (res != null) {
                RecordMetadata rmd = res.getRecordMetadata();
                ProducerRecord<String, Object> pr = res.getProducerRecord();
                log.info("Topic: {}, Sent message: {}, Offset: {} ", rmd.topic(), pr.value(), rmd.offset());
            } else if (ex != null) {
                result = "FAILED";
                log.info("Unable to send message={} due to : {} ", msg, ex.getMessage());
            }
            return result;
        };
        singlePartitionTopicKafkaTemplate.send(message).handleAsync(processResult.apply(message));
        Message<String> partition0 = getMessage(0);
        Message<String> partition1 = getMessage(1);
        Message<String> partition2 = getMessage(2);
        multiPartitionTopicKafkaTemplate.send(partition0).handleAsync(processResult.apply(partition0));
        multiPartitionTopicKafkaTemplate.send(partition1).handleAsync(processResult.apply(partition1));
        multiPartitionTopicKafkaTemplate.send(partition2).handleAsync(processResult.apply(partition2));
    }

    private static Message<String> getMessage(int partitionId) {
        return MessageBuilder.withPayload("It's 2024, we made it - " + partitionId)
                .setHeader(KafkaHeaders.PARTITION, partitionId)
                .setHeader("source", "Spring")
                .build();
    }
}
