package org.sk.springkafka.config;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    @Autowired
    private KafkaProperties kafkaProperties;

    @Autowired
    private Environment env;

    @Bean("filterKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, String> filterKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        // Records with India keyword will be consumed rest are skipped
        factory.setRecordFilterStrategy(record -> !record.value().contains("India"));
        return factory;
    }

    @Bean("defaultKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, String> defaultKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return factory;
    }

    @Bean("consumerFactory")
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        if (env.getActiveProfiles()[0].equals("local")) {
            props.put("bootstrap.servers", "http://localhost:9092");
        } else {
            props.put("bootstrap.servers", "https://gentle-macaw-10295-us1-kafka.upstash.io:9092");
            props.put("sasl.mechanism", "SCRAM-SHA-256");
            props.put("security.protocol", "SASL_SSL");
            // Provide your username and password
            props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"Z2VudGxlLW1hY2F3LTEwMjk1JM9RJYJKIkuF54sZmv5dIuwfLnQ8yRrgMOXSzQI\" password=\"ZTMxMTA2MzUtZTI2ZC00MGQ5LWExZDUtZDVhYTM3YzkwNGVi\";");
        }
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "g1");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new DefaultKafkaConsumerFactory<>(props);
    }


}
