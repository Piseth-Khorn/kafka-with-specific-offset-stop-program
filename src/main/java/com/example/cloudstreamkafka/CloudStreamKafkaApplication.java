package com.example.cloudstreamkafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class CloudStreamKafkaApplication {

    public static void main(String[] args) {
        SpringApplication.run(CloudStreamKafkaApplication.class, args);
    }

}
