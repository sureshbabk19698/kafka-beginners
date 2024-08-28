package org.sk.producer;

import org.sk.model.MessageWrapper;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class MultiPartitionTopicProducerTemplate extends KafkaProducerTemplate {

    public MultiPartitionTopicProducerTemplate(@Qualifier("multiPartitionTopicTemplate") KafkaTemplate<String, Object> kafkaTemplate) {
        super(kafkaTemplate);
    }

    @Override
    protected void processJsonResult(Map<String, Object> input) {
        String jsonValue = (String) input.get(JSON_VALUE);
        for (int i = 0; i < 3; i++) {
            MessageWrapper result = new MessageWrapper();
            result.setMessage(getMessage(jsonValue, i));
            sendKafkaMsg(result);
        }
    }

    @Override
    public String getTopicType() {
        return "MULTI";
    }

}
