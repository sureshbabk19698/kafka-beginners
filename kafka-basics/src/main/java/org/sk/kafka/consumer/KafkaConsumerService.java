package org.sk.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.sk.config.KafkaTopic;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class KafkaConsumerService {

    @KafkaListener(topics = KafkaTopic.MULTI_PARTITION_TOPIC, groupId = "MG-1")
    public void allPartitionMultiTopicListener(@Payload String message,
                                               @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) {
        //  MG-1: partitions assigned: [multi_partition_topic-0, multi_partition_topic-1, multi_partition_topic-2]
        log.info("Inside allPartitionMultiTopicListener: Message {} & partition id {}", message, partition);
    }


    @KafkaListener(topicPartitions = @TopicPartition(topic = KafkaTopic.MULTI_PARTITION_TOPIC, partitions = {"0, 1"}), groupId = "MG-2")
    public void partitionZeroAndOneMultiTopicListener(@Payload String message,
                                                     @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) {
        log.info("Inside partitionZeroAndOneMultiTopicListener: Message {} & partition id {}", message, partition);
    }

    @KafkaListener(topicPartitions = @TopicPartition(topic = KafkaTopic.MULTI_PARTITION_TOPIC, partitions = {"2"}),
            groupId = "MG-2")
    public void partitionTwoMultiTopicListener(@Payload String message,
                                                 @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) {
        log.info("Inside partitionTwoMultiTopicListener: Message {} & partition id {}", message, partition);
    }

    @KafkaListener(topics = KafkaTopic.SINGLE_PARTITION_TOPIC, groupId = "SG-1")
    public void singleTopicListener(@Payload String message,
                                    @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) {
        log.info("Inside singleTopicListener: Message {} & Partition Id {}", message, partition);
    }


}
