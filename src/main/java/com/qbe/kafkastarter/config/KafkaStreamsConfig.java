package com.qbe.kafkastarter.config;

import com.qbe.kafkastarter.topology.ProductMasterTopology;
import com.qbe.kafkastarter.topology.StockAlertActionTopology;
import com.qbe.kafkastarter.topology.StockAlertTopology;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.StreamsBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@Configuration
@EnableKafkaStreams
@RequiredArgsConstructor
public class KafkaStreamsConfig {

    private final ProductMasterTopology productMasterTopology;
    private final StockAlertTopology stockAlertTopology;
    private final StockAlertActionTopology stockAlertActionTopology;

    @Bean
    public Object topologyBean(StreamsBuilder builder) {

        productMasterTopology.build(builder);
        stockAlertTopology.build(builder);
        stockAlertActionTopology.build(builder);

        return builder.build();
    }
}
