package com.qbe.kafkastarter.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.qbe.avro.StockAlertState;
import java.util.List;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

@ExtendWith(MockitoExtension.class)
class TestStockAlertService {

    @Mock
    private StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    @Mock
    private KafkaStreams kafkaStreams;

    @Mock
    private ReadOnlyKeyValueStore<String, StockAlertState> store;

    @Mock
    private KeyValueIterator<String, StockAlertState> iterator;

    @InjectMocks
    private StockAlertService stockAlertService;

    @Test
    void shouldReturnEmptyListWhenKafkaStreamsIsNull() {
        when(streamsBuilderFactoryBean.getKafkaStreams()).thenReturn(null);

        List<StockAlertState> result = stockAlertService.findAll();
        assertThat(result).isEmpty();
    }

    @Test
    void shouldReturnAllAlerts() {
        StockAlertState alert1 = StockAlertState.newBuilder()
                .setAlertActive(true)
                .setSeverity("WARNING")
                .setLastKnownStock(10)
                .setLastAlertDate("2026-09-01T08:00:00Z")
                .build();

        StockAlertState alert2 = StockAlertState.newBuilder()
                .setAlertActive(true)
                .setSeverity("CRITICAL")
                .setLastKnownStock(2)
                .setLastAlertDate("2026-09-01T08:30:00Z")
                .build();

        when(streamsBuilderFactoryBean.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.store(any())).thenReturn(store);
        when(store.all()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true, true, false);
        when(iterator.next())
                .thenReturn(org.apache.kafka.streams.KeyValue.pair("P001", alert1))
                .thenReturn(org.apache.kafka.streams.KeyValue.pair("P002", alert2));

        List<StockAlertState> result = stockAlertService.findAll();

        assertThat(result).hasSize(2);
        assertThat(result.getFirst().getProductId()).isEqualTo("P001");
        assertThat(result.getFirst().getAlertActive()).isTrue();
        assertThat(result.getFirst().getSeverity()).isEqualTo("WARNING");
        assertThat(result.get(1).getProductId()).isEqualTo("P002");
        assertThat(result.get(1).getAlertActive()).isTrue();
        assertThat(result.get(1).getSeverity()).isEqualTo("CRITICAL");
        verify(iterator).close();
    }

    @Test
    void shouldReturnEmptyListWhenStoreContainsNoAlert() {
        when(streamsBuilderFactoryBean.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.store(any())).thenReturn(store);
        when(store.all()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(false);

        List<StockAlertState> result = stockAlertService.findAll();
        assertThat(result).isEmpty();
        verify(iterator).close();
    }

    @Test
    void shouldReturnNullWhenKafkaStreamsIsNullForProductLookup() {
        when(streamsBuilderFactoryBean.getKafkaStreams()).thenReturn(null);
        StockAlertState result = stockAlertService.findByProductId("P001");
        assertThat(result).isNull();
    }

    @Test
    void shouldReturnAlertForProductId() {
        StockAlertState alert = StockAlertState.newBuilder()
                .setAlertActive(true)
                .setSeverity("CRITICAL")
                .setLastKnownStock(3)
                .setLastAlertDate("2026-09-01T09:00:00Z")
                .build();

        when(streamsBuilderFactoryBean.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.store(any())).thenReturn(store);
        when(store.get("P001")).thenReturn(alert);

        StockAlertState result = stockAlertService.findByProductId("P001");
        assertNotNull(result);

        assertAll(
                () -> assertThat(result.getProductId()).isEqualTo("P001"),
                () -> assertThat(result.getAlertActive()).isTrue(),
                () -> assertThat(result.getSeverity()).isEqualTo("CRITICAL"),
                () -> assertThat(result.getLastKnownStock()).isEqualTo(3),
                () -> assertThat(result.getLastAlertDate()).isEqualTo("2026-09-01T09:00:00Z"));
    }

    @Test
    void shouldReturnNullWhenProductDoesNotExist() {
        when(streamsBuilderFactoryBean.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.store(any())).thenReturn(store);
        when(store.get("UNKNOWN")).thenReturn(null);

        StockAlertState result = stockAlertService.findByProductId("UNKNOWN");
        assertThat(result).isNull();
    }

    @Test
    void shouldPopulateProductIdFromStoreKey() {
        StockAlertState alert =
                StockAlertState.newBuilder().setAlertActive(true).build();

        when(streamsBuilderFactoryBean.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.store(any())).thenReturn(store);
        when(store.get("P001")).thenReturn(alert);

        StockAlertState result = stockAlertService.findByProductId("P001");
        assertThat(result.getProductId()).isEqualTo("P001");
    }
}
