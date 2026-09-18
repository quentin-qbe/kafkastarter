package com.qbe.kafkastarter.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Value("${spring.application.version}")
    private String version;

    @Bean
    public OpenAPI kafkaStarterOpenApi() {

        return new OpenAPI()
                .info(new Info()
                        .title("Kafka Product Catalog Aggregator")
                        .description(
                                """
                                Demo application based on:

                                - Spring Boot
                                - Kafka
                                - Kafka Streams
                                - Product Catalog Aggregation

                                Used to publish product events and build a consolidated Product Master.
                                """)
                        .version(version)
                        .contact(new Contact().name("QBE")));
    }
}
