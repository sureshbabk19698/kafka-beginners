package org.sk;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;

@SpringBootApplication
public class KafkaWebApplication extends SpringBootServletInitializer {

    public static void main(String[] args) {
        start(args);
    }

    public static void start(String[] args) {
        SpringApplication.run(KafkaWebApplication.class, args);
    }

    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder builder) {
        return builder.sources(KafkaWebApplication.class);
    }

}