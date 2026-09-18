package com.qbe.kafkastarter.config;

import com.qbe.avro.*;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Map;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AvroSerdeConfig {

    @Value("${spring.kafka.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    private <T extends SpecificRecord> SpecificAvroSerde<T> buildSerde() {
        SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
        serde.configure(Map.of("schema.registry.url", schemaRegistryUrl), false);
        return serde;
    }

    @Bean
    public SpecificAvroSerde<Product> productSerde() {
        return buildSerde();
    }

    @Bean
    public SpecificAvroSerde<Price> priceSerde() {
        return buildSerde();
    }

    @Bean
    public SpecificAvroSerde<Stock> stockSerde() {
        return buildSerde();
    }

    @Bean
    public SpecificAvroSerde<Supplier> supplierSerde() {
        return buildSerde();
    }

    @Bean
    public SpecificAvroSerde<Marketing> marketingSerde() {
        return buildSerde();
    }

    @Bean
    public SpecificAvroSerde<ProductMaster> productMasterSerde() {
        return buildSerde();
    }

    @Bean
    public SpecificAvroSerde<StockAlert> stockAlertSerde() {
        return buildSerde();
    }

    @Bean
    public SpecificAvroSerde<StockAlertState> stockAlertStateSerde() {
        return buildSerde();
    }

    @Bean
    public SpecificAvroSerde<StockAlertAction> stockAlertActionSerde() {
        return buildSerde();
    }
}
