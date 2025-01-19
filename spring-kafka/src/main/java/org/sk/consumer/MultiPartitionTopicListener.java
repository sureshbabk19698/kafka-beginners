package org.sk.consumer;

import lombok.extern.slf4j.Slf4j;
import org.sk.config.KafkaTopic;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class MultiPartitionTopicListener extends KafkaListenerService {

    @KafkaListener(topics = KafkaTopic.MULTI_PARTITION_TOPIC, groupId = "MG-1", containerFactory = "concurrentKafkaListenerContainerFactory")
    public void allPartitionMultiTopicListener(@Payload String message,
                                               @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                               @Headers MessageHeaders messageHeaders) {
        //  MG-1: partitions assigned: [multi_partition_topic-0, multi_partition_topic-1, multi_partition_topic-2]
        consume(message, partition, messageHeaders);
    }


//    @KafkaListener(topicPartitions = @TopicPartition(topic = KafkaTopic.MULTI_PARTITION_TOPIC, partitions = {"0, 1"}), groupId = "MG-2")
    public void partitionZeroAndOneMultiTopicListener(@Payload String message,
                                                      @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                                      @Headers MessageHeaders messageHeaders) {
        consume(message, partition, messageHeaders);
    }

//    @KafkaListener(topicPartitions = @TopicPartition(topic = KafkaTopic.MULTI_PARTITION_TOPIC, partitions = {"2"}), groupId = "MG-2")
    public void partitionTwoMultiTopicListener(@Payload String message,
                                               @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                               @Headers MessageHeaders messageHeaders) {
        consume(message, partition, messageHeaders);
    }

    @Override
    protected void process(String message) {
        // do Nothing
    }

}
