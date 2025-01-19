package org.sk.producer;

import org.sk.model.MessageWrapper;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class SingleFilterPartitionTopicProducerTemplate extends KafkaProducerTemplate {
    public SingleFilterPartitionTopicProducerTemplate(@Qualifier("singleFilterPartitionTopicTemplate") KafkaTemplate<String, Object> kafkaTemplate) {
        super(kafkaTemplate);
    }

    @Override
    protected void processJsonResult(Map<String, Object> input) {
        MessageWrapper result = new MessageWrapper();
        String jsonValue = (String) input.get(JSON_VALUE);
        result.setMessage(getMessage(jsonValue, 0));
        sendKafkaMsg(result);
    }

    @Override
    public String getTopicType() {
        return "FILTER";
    }

}
