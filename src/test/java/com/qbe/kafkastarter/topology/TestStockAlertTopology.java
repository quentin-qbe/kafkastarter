package com.qbe.kafkastarter.topology;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.qbe.avro.ProductMaster;
import com.qbe.avro.StockAlert;
import com.qbe.avro.StockAlertState;
import com.qbe.kafkastarter.constants.StateStoreConstants;
import com.qbe.kafkastarter.constants.TopicsConstants;
import com.qbe.kafkastarter.enums.StockAlertSeverity;
import com.qbe.kafkastarter.service.MetricsService;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.micrometer.core.instrument.Timer;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TestStockAlertTopology {

    private TopologyTestDriver testDriver;

    private TestInputTopic<String, ProductMaster> productMasterInput;
    private TestOutputTopic<String, StockAlert> stockAlertOutput;

    private KeyValueStore<String, StockAlertState> stockAlertStore;

    @Mock
    private MetricsService metricsService;

    @Mock
    private Timer.Sample sample;

    @BeforeEach
    void setup() {
        SpecificAvroSerde<ProductMaster> productMasterSerde = buildSerde();
        SpecificAvroSerde<StockAlert> stockAlertSerde = buildSerde();
        SpecificAvroSerde<StockAlertState> stockAlertStateSerde = buildSerde();

        StockAlertTopology topology =
                new StockAlertTopology(productMasterSerde, stockAlertSerde, stockAlertStateSerde, metricsService);

        StreamsBuilder builder = new StreamsBuilder();
        topology.build(builder);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-stock-alert");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

        testDriver = new TopologyTestDriver(builder.build(), props);

        productMasterInput = testDriver.createInputTopic(
                TopicsConstants.TOPIC_MASTER_PRODUCT, new StringSerializer(), productMasterSerde.serializer());

        stockAlertOutput = testDriver.createOutputTopic(
                TopicsConstants.TOPIC_STOCK_ALERT, new StringDeserializer(), stockAlertSerde.deserializer());

        stockAlertStore = testDriver.getKeyValueStore(StateStoreConstants.STOCK_ALERT_STORE);

        Mockito.when(metricsService.startTimer()).thenReturn(sample);
    }

    @Test
    void shouldCreateStockAlertWhenStockBelowThreshold() {
        productMasterInput.pipeInput("P001", productMaster(5));

        StockAlert alert = stockAlertOutput.readValue();

        assertThat(alert)
                .isNotNull()
                .extracting(
                        StockAlert::getProductId,
                        StockAlert::getSku,
                        StockAlert::getCurrentStock,
                        StockAlert::getThreshold,
                        StockAlert::getSeverity)
                .containsExactly("P001", "KB001", 5, 10, StockAlertSeverity.CRITICAL.name());

        verify(metricsService).startTimer();
        verify(metricsService).stopStockAlertTimer(sample);
        verify(metricsService).incrementStockAlert(StockAlertSeverity.CRITICAL.name());
    }

    @Test
    void shouldNotCreateStockAlertWhenStockAboveThreshold() {
        productMasterInput.pipeInput("P001", productMaster(50));

        assertThat(stockAlertOutput.isEmpty()).isTrue();
    }

    @Test
    void shouldCreateCriticalAlertWhenStockVeryLow() {
        productMasterInput.pipeInput("P001", productMaster(2));

        StockAlert alert = stockAlertOutput.readValue();

        assertThat(alert.getSeverity()).isEqualTo(StockAlertSeverity.CRITICAL.name());
        verify(metricsService).incrementStockAlert(StockAlertSeverity.CRITICAL.name());
    }

    @Test
    void shouldCreateOutOfStockAlertWhenNoStockRemaining() {
        productMasterInput.pipeInput("P001", productMaster(0));

        StockAlert alert = stockAlertOutput.readValue();

        assertThat(alert.getSeverity()).isEqualTo(StockAlertSeverity.OUT_OF_STOCK.name());
        verify(metricsService).incrementStockAlert(StockAlertSeverity.OUT_OF_STOCK.name());
    }

    @Test
    void shouldSendOnlyOneAlertWhileStockRemainsLow() {
        productMasterInput.pipeInput("P001", productMaster(9));
        productMasterInput.pipeInput("P001", productMaster(8));
        productMasterInput.pipeInput("P001", productMaster(7));

        assertThat(stockAlertOutput.getQueueSize()).isEqualTo(1);
    }

    @Test
    void shouldGenerateNewAlertAfterRecovery() {
        productMasterInput.pipeInput("P001", productMaster(9));
        productMasterInput.pipeInput("P001", productMaster(15));
        productMasterInput.pipeInput("P001", productMaster(8));

        assertThat(stockAlertOutput.getQueueSize()).isEqualTo(2);
    }

    @Test
    void shouldStoreAlertStateWhenAlertIsTriggered() {
        productMasterInput.pipeInput("P001", productMaster(5));

        StockAlertState state = stockAlertStore.get("P001");

        assertThat(state).isNotNull();

        assertAll(
                () -> assertThat(state.getAlertActive()).isTrue(),
                () -> assertThat(state.getSeverity()).isEqualTo("CRITICAL"),
                () -> assertThat(state.getLastKnownStock()).isEqualTo(5),
                () -> assertThat(state.getLastAlertDate()).isNotNull());
    }

    @Test
    void shouldDeactivateAlertWhenStockReturnsToNormal() {
        productMasterInput.pipeInput("P001", productMaster(5));
        productMasterInput.pipeInput("P001", productMaster(20));

        StockAlertState state = stockAlertStore.get("P001");

        assertAll(() -> assertThat(state.getAlertActive()).isFalse(), () -> assertThat(state.getLastKnownStock())
                .isEqualTo(20));
    }

    @Test
    void shouldKeepLastAlertInformationAfterRecovery() {
        productMasterInput.pipeInput("P001", productMaster(2));
        productMasterInput.pipeInput("P001", productMaster(25));

        StockAlertState state = stockAlertStore.get("P001");

        assertAll(
                () -> assertThat(state.getAlertActive()).isFalse(),
                () -> assertThat(state.getSeverity()).isEqualTo("CRITICAL"),
                () -> assertThat(state.getLastAlertDate()).isNotNull());
    }

    @Test
    void shouldPublishMetricsWhenAlertCreated() {
        productMasterInput.pipeInput("P001", productMaster(5));

        assertThat(stockAlertOutput.isEmpty()).isFalse();
        verify(metricsService).startTimer();
        verify(metricsService).stopStockAlertTimer(sample);
        verify(metricsService).incrementStockAlert(StockAlertSeverity.CRITICAL.name());
    }

    @Test
    void shouldNotPublishAlertMetricWhenStockAboveThreshold() {
        productMasterInput.pipeInput("P001", productMaster(50));

        verify(metricsService).startTimer();
        verify(metricsService).stopStockAlertTimer(sample);
        verify(metricsService, never()).incrementStockAlert(anyString());
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    private ProductMaster productMaster(int stock) {
        return ProductMaster.newBuilder()
                .setProductId("P001")
                .setSku("KB001")
                .setName("Keyboard")
                .setBrand("Microsoft")
                .setCategory("Accessories")
                .setActive(true)
                .setPrice(49.99d)
                .setCurrency("EUR")
                .setAvailableStock(stock)
                .setReservedStock(0)
                .setSupplierId("SUP001")
                .setSupplierName("TechSupplier")
                .setSupplierCountry("FR")
                .setShortDescription("Mechanical keyboard")
                .setLongDescription("Premium mechanical keyboard")
                .setTags(List.of("keyboard", "gaming"))
                .setAlertThreshold(10)
                .setCreatedAt(Instant.parse("2026-08-29T10:00:00Z"))
                .setLastUpdatedAt(Instant.parse("2026-08-31T10:03:00Z"))
                .build();
    }

    private <T extends SpecificRecord> SpecificAvroSerde<T> buildSerde() {
        Map<String, String> serdeConfig = Map.of("schema.registry.url", "mock://test");

        SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();

        serde.configure(serdeConfig, false);
        return serde;
    }
}
