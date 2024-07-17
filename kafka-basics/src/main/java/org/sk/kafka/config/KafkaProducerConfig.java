package org.sk.kafka.config;


import org.sk.config.KafkaTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
public class KafkaProducerConfig {

    @Autowired
    private KafkaProperties kafkaProperties;

    @Bean("singlePartitionTopicKafkaTemplate")
    public KafkaTemplate<String, Object> singlePartitionTopicKafkaTemplate() {
        KafkaTemplate<String, Object> kafkaTemplate = new KafkaTemplate<>(producerFactory());
        kafkaTemplate.setDefaultTopic(KafkaTopic.SINGLE_PARTITION_TOPIC);
        return kafkaTemplate;
    }

    @Bean("multiPartitionTopicKafkaTemplate")
    public KafkaTemplate<String, Object> multiPartitionTopicKafkaTemplate() {
        KafkaTemplate<String, Object> kafkaTemplate = new KafkaTemplate<>(producerFactory());
        kafkaTemplate.setDefaultTopic(KafkaTopic.MULTI_PARTITION_TOPIC);
        return kafkaTemplate;
    }

    @Bean("singleFilterPartitionTopicKafkaTemplate")
    public KafkaTemplate<String, Object> singleFilterPartitionTopicKafkaTemplate() {
        KafkaTemplate<String, Object> kafkaTemplate = new KafkaTemplate<>(producerFactory());
        kafkaTemplate.setDefaultTopic(KafkaTopic.SINGLE_FILTER_PARTITION_TOPIC);
        return kafkaTemplate;
    }

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        return new DefaultKafkaProducerFactory<>(kafkaProperties.buildProducerProperties(null));
    }


}
