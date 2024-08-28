package org.sk.factory;

import org.sk.producer.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class KafkaProducerFactory {

    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private List<KafkaProducerTemplate> producerTemplates;

    public KafkaProducerTemplate getListener(String type) {
        return producerTemplates.stream().filter(p -> p.getTopicType().equalsIgnoreCase(type)).findFirst()
                .orElseThrow();
    }

}
