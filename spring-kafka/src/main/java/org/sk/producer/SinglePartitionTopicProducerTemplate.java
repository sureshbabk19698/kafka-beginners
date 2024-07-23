package org.sk.producer;

import org.sk.model.MessageWrapper;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
public class SinglePartitionTopicProducerTemplate extends KafkaProducerTemplate {

    public SinglePartitionTopicProducerTemplate(@Qualifier("singlePartitionTopicTemplate") KafkaTemplate<String, Object> kafkaTemplate) {
        super(kafkaTemplate);
    }

    @Override
    protected List<MessageWrapper> processJsonResult(Map<String, Object> input) {
        MessageWrapper result = new MessageWrapper();
        String jsonValue = (String) input.get(JSON_VALUE);
        result.setMessage(getMessage(jsonValue, null, kafkaTemplate));
        result.setStatus(SUCCESS);
        return List.of(result);
    }

}
