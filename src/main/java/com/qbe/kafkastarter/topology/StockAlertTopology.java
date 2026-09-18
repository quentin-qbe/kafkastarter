package com.qbe.kafkastarter.topology;

import com.qbe.avro.ProductMaster;
import com.qbe.avro.StockAlert;
import com.qbe.avro.StockAlertState;
import com.qbe.kafkastarter.constants.StateStoreConstants;
import com.qbe.kafkastarter.constants.TopicsConstants;
import com.qbe.kafkastarter.processor.StockAlertProcessor;
import com.qbe.kafkastarter.service.MetricsService;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class StockAlertTopology {

    private final SpecificAvroSerde<ProductMaster> productMasterSerde;
    private final SpecificAvroSerde<StockAlert> stockAlertSerde;
    private final SpecificAvroSerde<StockAlertState> stockAlertStateSerde;

    private final MetricsService metricsService;

    public void build(StreamsBuilder builder) {

        StoreBuilder<KeyValueStore<String, StockAlertState>> storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(StateStoreConstants.STOCK_ALERT_STORE),
                Serdes.String(),
                stockAlertStateSerde);
        builder.addStateStore(storeBuilder);

        builder.stream(TopicsConstants.TOPIC_MASTER_PRODUCT, Consumed.with(Serdes.String(), productMasterSerde))
                .processValues(() -> new StockAlertProcessor(metricsService), StateStoreConstants.STOCK_ALERT_STORE)
                .peek((k, v) -> {
                    log.info("STOCK_ALERT => {} => {}", k, v);
                    metricsService.incrementStockAlert(v.getSeverity());
                })
                .to(TopicsConstants.TOPIC_STOCK_ALERT, Produced.with(Serdes.String(), stockAlertSerde));
    }
}
