package org.sk.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.sk.config.KafkaConfig;
import org.sk.config.KafkaTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class ProducerDemoWithoutKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithoutKeys.class.getSimpleName());

    public static void main(String[] args) {
        args = Objects.nonNull(args) && args.length > 0 ? args : new String[]{"--env=local"};
        log.info("Inside Kafka Producer Demo");
        var props = KafkaConfig.setUpProducer(args[0].split("=")[1]);
        props.put("batch.size", "2");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        for (int j = 1; j <= 3; j++) {
            for (int i = 1; i <= 2; i++) {
                ProducerRecord<String, String> pr = new ProducerRecord<>(KafkaTopic.MULTI_PARTITION_TOPIC, "Its 2024, I made it." + i);
                producer.send(pr, (RecordMetadata metadata, Exception exception) -> log.info("Partition {} ", metadata.partition()));
            }
        }
        producer.flush();
        producer.close();
    }
}
