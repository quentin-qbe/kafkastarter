package com.qbe.kafkastarter;

import org.springframework.boot.SpringApplication;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
public class TestKafkaStarterApplication {

    public static void main(String[] args) {
        SpringApplication.from(KafkaStarterApplication::main)
                .with(TestcontainersConfiguration.class)
                .run(args);
    }
}
