package org.sk.controller;

import org.sk.factory.KafkaProducerFactory;
import org.sk.model.Customer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
public class KafkaController {

    @Autowired
    private KafkaProducerFactory kafkaProducerFactory;

    @PostMapping(value = "/publish", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    public String publish(@RequestParam(value = "topicType", required = false) String topicType,
                          @RequestBody Customer customer) {
        Map<String, Object> inpMap = new HashMap<>();
        inpMap.put("SOURCE", customer);
        kafkaProducerFactory.getListener(topicType).publishMessage(inpMap);
        return "Success";
    }

}
