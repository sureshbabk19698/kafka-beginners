package org.sk.config;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaConfig {


    public static Properties setUpProducer(String env) {
        var props = new Properties();
        if (env.equals("local")) {
            props.put("bootstrap.servers", "http://localhost:9092");
        } else {
            props.put("bootstrap.servers", "https://gentle-macaw-10295-us1-kafka.upstash.io:9092");
            props.put("sasl.mechanism", "SCRAM-SHA-256");
            props.put("security.protocol", "SASL_SSL");
            props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"***\" password=\"**");
        }
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
//        props.put("partitioner.class", RoundRobinPartitioner.class.getName()); // Less number of batches leads to more requests to partitions
//        props.put("batch.size", "200"); // default batch.size = 16384 // mores number of batches leads to less requests to partitions -- StickyPartitioner
        return props;
    }

    public static Properties setUpConsumer(String env) {
        var props = new Properties();
        if (env.equals("local")) {
            props.put("bootstrap.servers", "http://localhost:9092");
        } else {
            props.put("bootstrap.servers", "https://gentle-macaw-10295-us1-kafka.upstash.io:9092");
            props.put("sasl.mechanism", "SCRAM-SHA-256");
            props.put("security.protocol", "SASL_SSL");
            // Provide your username and password
            props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"***\" password=\"**");
        }
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("group.id", "g1");
        props.put("auto.offset.reset", "earliest");
        return props;
    }

}
