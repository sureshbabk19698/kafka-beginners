package org.sk.kafka.controller;

import org.sk.kafka.producer.KafkaProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class KafkaController {

    @Autowired
    private KafkaProducerService kafkaProducerService;

    @PostMapping(value = "/publish", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    public String publish(@RequestParam(value = "topicType", required = false) String topicTYpe) {

        switch (topicTYpe) {
            case "SINGLE" -> kafkaProducerService.singlePartitionTopicPublish();
            case "FILTER" -> kafkaProducerService.singlePartitionTopicWithEventTypePublish();
            case "MULTI" -> kafkaProducerService.multiPartitionTopicPublish();
            default -> kafkaProducerService.publishMessage();
        }

        return "Success";
    }

}
