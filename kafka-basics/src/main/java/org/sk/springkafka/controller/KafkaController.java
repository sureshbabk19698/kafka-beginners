package org.sk.springkafka.controller;

import org.sk.springkafka.model.Customer;
import org.sk.springkafka.producer.KafkaProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class KafkaController {

    @Autowired
    private KafkaProducerService kafkaProducerService;

    @PostMapping(value = "/publish", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    public String publish(@RequestParam(value = "topicType", required = false) String topicTYpe,
                          @RequestBody Customer customer) {

        switch (topicTYpe) {
            case "SINGLE" -> kafkaProducerService.singlePartitionTopicPublish(customer);
            case "FILTER" -> kafkaProducerService.singlePartitionTopicWithEventTypePublish(customer);
            case "MULTI" -> kafkaProducerService.multiPartitionTopicPublish(customer);
            case "DLT" -> kafkaProducerService.sourceDltTopicTemplatePublish(customer);
            default -> kafkaProducerService.publishMessage(customer);
        }

        return "Success";
    }

}
