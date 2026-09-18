package com.qbe.kafkastarter.topology;

import com.qbe.avro.StockAlertAction;
import com.qbe.kafkastarter.constants.TopicsConstants;
import com.qbe.kafkastarter.processor.StockAlertActionProcessor;
import com.qbe.kafkastarter.service.DeadLetterService;
import com.qbe.kafkastarter.service.MetricsService;
import com.qbe.kafkastarter.service.StockAlertActionService;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class StockAlertActionTopology {

    private final SpecificAvroSerde<StockAlertAction> stockAlertActionSerde;
    private final StockAlertActionService service;
    private final DeadLetterService deadLetterService;
    private final MetricsService metricsService;

    public void build(StreamsBuilder builder) {
        builder.stream(TopicsConstants.TOPIC_STOCK_ALERT_ACTIONS, Consumed.with(Serdes.String(), stockAlertActionSerde))
                .peek((k, v) -> {
                    log.info("STOCK_ALERT_ACTION => {} => {}", k, v);
                })
                .processValues(() -> new StockAlertActionProcessor(service, metricsService, deadLetterService));
    }
}
