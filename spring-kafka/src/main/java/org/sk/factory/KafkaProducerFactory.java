package org.sk.factory;

import org.sk.producer.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

@Component
public class KafkaProducerFactory {

    @Autowired
    private ApplicationContext applicationContext;
    public KafkaProducerTemplate getListener(String type) {
        return switch (type) {
            case "SINGLE" -> applicationContext.getBean(SinglePartitionTopicProducerTemplate.class);
            case "FILTER" -> applicationContext.getBean(SingleFilterPartitionTopicProducerTemplate.class);
            case "MULTI" -> applicationContext.getBean(MultiPartitionTopicProducerTemplate.class);
            case "DLT" -> applicationContext.getBean(SourceDLTPartitionTopicProducerTemplate.class);
            default -> throw new IllegalStateException("Unexpected value: " + type);
        };
    }
}
