package org.sk.springkafka.config;


import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.sk.config.KafkaTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Autowired
    private KafkaProperties kafkaProperties;

    @Autowired
    private Environment env;

    @Bean("singlePartitionTopicTemplate")
    public KafkaTemplate<String, Object> singlePartitionTopicTemplate() {
        KafkaTemplate<String, Object> kafkaTemplate = new KafkaTemplate<>(producerFactory());
        kafkaTemplate.setDefaultTopic(KafkaTopic.SINGLE_PARTITION_TOPIC);
        return kafkaTemplate;
    }

    @Bean("multiPartitionTopicTemplate")
    public KafkaTemplate<String, Object> multiPartitionTopicTemplate() {
        KafkaTemplate<String, Object> kafkaTemplate = new KafkaTemplate<>(producerFactory());
        kafkaTemplate.setDefaultTopic(KafkaTopic.MULTI_PARTITION_TOPIC);
        return kafkaTemplate;
    }

    @Bean("singleFilterPartitionTopicTemplate")
    public KafkaTemplate<String, Object> singleFilterPartitionTopicTemplate() {
        KafkaTemplate<String, Object> kafkaTemplate = new KafkaTemplate<>(producerFactory());
        kafkaTemplate.setDefaultTopic(KafkaTopic.SINGLE_FILTER_PARTITION_TOPIC);
        return kafkaTemplate;
    }

    @Bean("producerFactory")
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        if (env.getActiveProfiles()[0].equals("local")) {
            props.put("bootstrap.servers", "http://localhost:9092");
        } else {
            props.put("bootstrap.servers", "https://gentle-macaw-10295-us1-kafka.upstash.io:9092");
            props.put("sasl.mechanism", "SCRAM-SHA-256");
            props.put("security.protocol", "SASL_SSL");
            props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"Z2VudGxlLW1hY2F3LTEwMjk1JM9RJYJKIkuF54sZmv5dIuwfLnQ8yRrgMOXSzQI\" password=\"ZTMxMTA2MzUtZTI2ZC00MGQ5LWExZDUtZDVhYTM3YzkwNGVi\";");
        }
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new DefaultKafkaProducerFactory<>(props);
    }


}
