package org.sk.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.sk.config.KafkaConfig;
import org.sk.config.KafkaTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

public class ConsumerDemoWithShutDown {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutDown.class.getSimpleName());

    public static void main(String[] args) {
        args = Objects.nonNull(args) && args.length > 0 ? args : new String[]{"--env=local"};
        log.info("Inside Kafka Consumer Demo");
        var props = KafkaConfig.setUpConsumer(args[0].split("=")[1]);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);


        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detected a Shutdown, lets exit by calling consumer.wakeup()...");
            consumer.wakeup();

            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        try {
            consumer.subscribe(List.of(KafkaTopic.FIRST_TOPIC));
            while (true) {
                log.info("Polling..");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    log.info("Topic: {}, Partition: {}, Key: {}, Value: {}", record.topic(), record.partition(), record.key(), record.value());
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer is starting to shutdown");
        } catch (Exception e) {
            log.info("Unexpected Exception {} ", e.getMessage());
        } finally {
            consumer.close(); // commits offset
            log.info("Gracefully shutdown consumer.");
        }
    }

}

