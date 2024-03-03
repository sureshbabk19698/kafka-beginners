package org.sk.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.sk.config.KafkaConfig;
import org.sk.config.KafkaTopic;
import org.sk.producer.ProducerDemoWithoutKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

public class ConsumerDemo {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithoutKeys.class.getSimpleName());

    public static void main(String[] args) {
        args = Objects.nonNull(args) && args.length > 0 ? args : new String[]{"--env=local"};
        log.info("Inside Kafka Consumer Demo");
        var props = KafkaConfig.setUpConsumer(args[0].split("=")[1]);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(KafkaTopic.THIRD_TOPIC));

        while (true) {
            log.info("Polling..");
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                log.info("Topic: {}, Partition: {}, Key: {}, Value: {}", record.topic(), record.partition(), record.key(), record.value());
            }
        }
    }

}

