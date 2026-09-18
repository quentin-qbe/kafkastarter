package com.qbe.kafkastarter.service;

import com.qbe.avro.StockAlertState;
import com.qbe.kafkastarter.constants.StateStoreConstants;
import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class StockAlertService {

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    public List<StockAlertState> findAll() {
        ReadOnlyKeyValueStore<String, StockAlertState> store = getStore();

        if (store == null) {
            return List.of();
        }

        List<StockAlertState> result = new ArrayList<>();
        try (KeyValueIterator<String, StockAlertState> iterator = store.all()) {
            while (iterator.hasNext()) {
                var entry = iterator.next();
                result.add(StockAlertState.newBuilder(entry.value)
                        .setProductId(entry.key)
                        .build());
            }
        }
        return result;
    }

    public StockAlertState findByProductId(String productId) {
        ReadOnlyKeyValueStore<String, StockAlertState> store = getStore();
        if (store == null) {
            return null;
        }

        StockAlertState state = store.get(productId);
        if (state == null) {
            return null;
        }
        return StockAlertState.newBuilder(state).setProductId(productId).build();
    }

    private ReadOnlyKeyValueStore<String, StockAlertState> getStore() {
        KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
        if (kafkaStreams == null) {
            return null;
        }
        try {
            return kafkaStreams.store(StoreQueryParameters.fromNameAndType(
                    StateStoreConstants.STOCK_ALERT_STORE, QueryableStoreTypes.keyValueStore()));
        } catch (InvalidStateStoreException e) {
            log.error("Failed to get store: {}", StateStoreConstants.STOCK_ALERT_STORE, e);
            return null;
        }
    }
}
