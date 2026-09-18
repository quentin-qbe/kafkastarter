package com.qbe.kafkastarter.topology;

import com.qbe.avro.*;
import com.qbe.kafkastarter.constants.TopicsConstants;
import com.qbe.kafkastarter.service.MetricsService;
import com.qbe.kafkastarter.utils.EnricherUtils;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class ProductMasterTopology {

    private final SpecificAvroSerde<Product> productSerde;
    private final SpecificAvroSerde<Price> priceSerde;
    private final SpecificAvroSerde<Stock> stockSerde;
    private final SpecificAvroSerde<Supplier> supplierSerde;
    private final SpecificAvroSerde<Marketing> marketingSerde;
    private final SpecificAvroSerde<ProductMaster> productMasterSerde;

    private final MetricsService metricsService;

    public KTable<String, ProductMaster> build(final StreamsBuilder builder) {

        KTable<String, Product> products =
                builder.table(TopicsConstants.TOPIC_PRODUCT, Consumed.with(Serdes.String(), productSerde));
        KTable<String, Price> prices =
                builder.table(TopicsConstants.TOPIC_PRICE, Consumed.with(Serdes.String(), priceSerde));
        KTable<String, Stock> stocks =
                builder.table(TopicsConstants.TOPIC_STOCK, Consumed.with(Serdes.String(), stockSerde));
        KTable<String, Supplier> suppliers =
                builder.table(TopicsConstants.TOPIC_SUPPLIER, Consumed.with(Serdes.String(), supplierSerde));
        KTable<String, Marketing> marketing =
                builder.table(TopicsConstants.TOPIC_MARKETING, Consumed.with(Serdes.String(), marketingSerde));
        KTable<String, ProductMaster> productMaster = products.leftJoin(prices, EnricherUtils::from)
                .leftJoin(stocks, EnricherUtils::enrichWithStock)
                .leftJoin(suppliers, EnricherUtils::enrichWithSupplier)
                .leftJoin(marketing, EnricherUtils::enrichWithMarketing);

        productMaster
                .toStream()
                .mapValues(value -> {
                    Timer.Sample sample = metricsService.startTimer();
                    try {
                        return value;
                    } finally {
                        metricsService.stopProductMasterTimer(sample);
                    }
                })
                .peek((k, v) -> {
                    log.info("PRODUCT_MASTER => {} => {}", k, v);
                    metricsService.incrementProductMaster();
                })
                .to(TopicsConstants.TOPIC_MASTER_PRODUCT, Produced.with(Serdes.String(), productMasterSerde));

        return productMaster;
    }
}
