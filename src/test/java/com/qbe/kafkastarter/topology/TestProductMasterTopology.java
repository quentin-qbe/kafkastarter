package com.qbe.kafkastarter.topology;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.mockito.Mockito.*;

import com.qbe.avro.*;
import com.qbe.kafkastarter.constants.TopicsConstants;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TestProductMasterTopology {

    private TopologyTestDriver testDriver;

    private TestInputTopic<String, Product> productInput;
    private TestInputTopic<String, Price> priceInput;
    private TestInputTopic<String, Stock> stockInput;
    private TestInputTopic<String, Supplier> supplierInput;
    private TestInputTopic<String, Marketing> marketingInput;
    private TestOutputTopic<String, ProductMaster> productMasterTopic;

    @Mock
    private MetricsService metricsService;

    @Mock
    private Timer.Sample sample;

    @BeforeEach
    void setup() {
        SpecificAvroSerde<Product> productSerde = buildSerde();
        SpecificAvroSerde<Price> priceSerde = buildSerde();
        SpecificAvroSerde<Stock> stockSerde = buildSerde();
        SpecificAvroSerde<Supplier> supplierSerde = buildSerde();
        SpecificAvroSerde<Marketing> marketingSerde = buildSerde();
        SpecificAvroSerde<ProductMaster> productMasterSerde = buildSerde();

        ProductMasterTopology topology = new ProductMasterTopology(
                productSerde,
                priceSerde,
                stockSerde,
                supplierSerde,
                marketingSerde,
                productMasterSerde,
                metricsService);

        StreamsBuilder builder = new StreamsBuilder();
        topology.build(builder);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

        testDriver = new TopologyTestDriver(builder.build(), props);

        productInput = testDriver.createInputTopic(
                TopicsConstants.TOPIC_PRODUCT, new StringSerializer(), productSerde.serializer());

        priceInput = testDriver.createInputTopic(
                TopicsConstants.TOPIC_PRICE, new StringSerializer(), priceSerde.serializer());

        stockInput = testDriver.createInputTopic(
                TopicsConstants.TOPIC_STOCK, new StringSerializer(), stockSerde.serializer());

        supplierInput = testDriver.createInputTopic(
                TopicsConstants.TOPIC_SUPPLIER, new StringSerializer(), supplierSerde.serializer());

        marketingInput = testDriver.createInputTopic(
                TopicsConstants.TOPIC_MARKETING, new StringSerializer(), marketingSerde.serializer());

        productMasterTopic = testDriver.createOutputTopic(
                TopicsConstants.TOPIC_MASTER_PRODUCT, new StringDeserializer(), productMasterSerde.deserializer());

        lenient().when(metricsService.startTimer()).thenReturn(sample);
    }

    @Test
    void shouldCreateProductMasterWhenProductReceived() {
        productInput.pipeInput("P001", product());
        assertThat(productMasterTopic.getQueueSize()).isEqualTo(1);

        ProductMaster result = productMasterTopic.readValue();
        assertAll(
                () -> assertThat(result.getProductId()).isEqualTo("P001"),
                () -> assertThat(result.getSku()).isEqualTo("KB001"),
                () -> assertThat(result.getName()).isEqualTo("Keyboard"),
                () -> assertThat(result.getPrice()).isEqualTo(0d),
                () -> assertThat(result.getSupplierId()).isNull());

        verify(metricsService, atLeastOnce()).incrementProductMaster();
        verify(metricsService, atLeastOnce()).startTimer();
        verify(metricsService, atLeastOnce()).stopProductMasterTimer(sample);
    }

    @Test
    void shouldAggregateAllTopicsIntoProductMaster() {
        productInput.pipeInput("P001", product());
        priceInput.pipeInput("P001", price());
        stockInput.pipeInput("P001", stock(100, 5));
        supplierInput.pipeInput("P001", supplier());
        marketingInput.pipeInput("P001", marketing());

        ProductMaster result = null;
        while (!productMasterTopic.isEmpty()) {
            result = productMasterTopic.readValue();
        }

        assertThat(result)
                .isNotNull()
                .extracting(
                        ProductMaster::getProductId,
                        ProductMaster::getPrice,
                        ProductMaster::getAvailableStock,
                        ProductMaster::getReservedStock,
                        ProductMaster::getSupplierId,
                        ProductMaster::getSupplierName,
                        ProductMaster::getSupplierCountry,
                        ProductMaster::getShortDescription)
                .containsExactly("P001", 49.99d, 100, 5, "SUP001", "TechSupplier", "FR", "Mechanical keyboard");

        verify(metricsService, atLeastOnce()).incrementProductMaster();
        verify(metricsService, atLeastOnce()).stopProductMasterTimer(sample);
    }

    @Test
    void shouldHandleOutOfOrderEvents() {
        priceInput.pipeInput("P001", price());
        assertThat(productMasterTopic.isEmpty()).isTrue();

        productInput.pipeInput("P001", product());

        ProductMaster result = null;
        while (!productMasterTopic.isEmpty()) {
            result = productMasterTopic.readValue();
        }

        assertThat(result)
                .isNotNull()
                .extracting(ProductMaster::getProductId, ProductMaster::getPrice)
                .containsExactly("P001", 49.99d);
    }

    @Test
    void shouldUpdateProductMasterWhenStockChanges() {
        productInput.pipeInput("P001", product());
        stockInput.pipeInput("P001", stock(100, 5));
        stockInput.pipeInput("P001", stock(50, 2));

        ProductMaster result = null;
        while (!productMasterTopic.isEmpty()) {
            result = productMasterTopic.readValue();
        }

        assertThat(result)
                .isNotNull()
                .extracting(
                        ProductMaster::getProductId, ProductMaster::getAvailableStock, ProductMaster::getReservedStock)
                .containsExactly("P001", 50, 2);
    }

    @Test
    void shouldUpdatePriceWhenNewPriceEventReceived() {
        productInput.pipeInput("P001", product());
        priceInput.pipeInput("P001", price());
        priceInput.pipeInput(
                "P001",
                Price.newBuilder()
                        .setProductId("P001")
                        .setAmount(10.99d)
                        .setCurrency("EUR")
                        .setLastUpdatedAt(Instant.parse("2026-08-31T11:00:00Z"))
                        .build());

        ProductMaster result = null;
        while (!productMasterTopic.isEmpty()) {
            result = productMasterTopic.readValue();
        }

        assertThat(result).isNotNull().extracting(ProductMaster::getPrice).isEqualTo(10.99d);

        verify(metricsService, atLeastOnce()).incrementProductMaster();
    }

    @Test
    void shouldHandleNullOptionalFields() {
        productInput.pipeInput(
                "P001",
                Product.newBuilder()
                        .setProductId("P001")
                        .setSku("KB001")
                        .setName("Keyboard")
                        .setBrand("Microsoft")
                        .setCategory("Accessories")
                        .setActive(true)
                        .setCreatedAt(Instant.parse("2026-08-29T10:00:00Z"))
                        .build());

        ProductMaster result = null;
        while (!productMasterTopic.isEmpty()) {
            result = productMasterTopic.readValue();
        }

        assertThat(result)
                .isNotNull()
                .extracting(
                        ProductMaster::getProductId,
                        ProductMaster::getPrice,
                        ProductMaster::getCurrency,
                        ProductMaster::getAvailableStock,
                        ProductMaster::getReservedStock,
                        ProductMaster::getSupplierId,
                        ProductMaster::getSupplierName,
                        ProductMaster::getSupplierCountry,
                        ProductMaster::getShortDescription,
                        ProductMaster::getLongDescription)
                .containsExactly("P001", 0d, null, null, null, null, null, null, null, null);
    }

    @Test
    void shouldIgnoreUnknownProduct() {
        priceInput.pipeInput(
                "UNKNOWN",
                Price.newBuilder()
                        .setProductId("UNKNOWN")
                        .setAmount(49.99d)
                        .setCurrency("EUR")
                        .setLastUpdatedAt(Instant.parse("2026-08-31T10:00:00Z"))
                        .build());

        assertThat(productMasterTopic.isEmpty()).isTrue();
    }

    @Test
    void shouldUpdateMarketingInformation() {
        productInput.pipeInput("P001", product());
        marketingInput.pipeInput("P001", marketing());

        marketingInput.pipeInput(
                "P001",
                Marketing.newBuilder()
                        .setProductId("P001")
                        .setShortDescription("Gaming keyboard")
                        .setLongDescription("Premium RGB gaming keyboard")
                        .setTags(List.of("gaming", "rgb"))
                        .setLastUpdatedAt(Instant.parse("2026-08-31T11:00:00Z"))
                        .build());

        ProductMaster result = null;
        while (!productMasterTopic.isEmpty()) {
            result = productMasterTopic.readValue();
        }

        assertThat(result)
                .isNotNull()
                .extracting(
                        ProductMaster::getShortDescription, ProductMaster::getLongDescription, ProductMaster::getTags)
                .containsExactly("Gaming keyboard", "Premium RGB gaming keyboard", List.of("gaming", "rgb"));
    }

    @Test
    void shouldPreserveExistingDataWhenPartialEventReceived() {
        productInput.pipeInput("P001", product());
        priceInput.pipeInput("P001", price());
        stockInput.pipeInput("P001", stock(100, 5));

        ProductMaster result = null;
        while (!productMasterTopic.isEmpty()) {
            result = productMasterTopic.readValue();
        }

        assertThat(result)
                .isNotNull()
                .extracting(
                        ProductMaster::getProductId,
                        ProductMaster::getName,
                        ProductMaster::getPrice,
                        ProductMaster::getCurrency,
                        ProductMaster::getAvailableStock,
                        ProductMaster::getReservedStock,
                        ProductMaster::getSupplierId,
                        ProductMaster::getSupplierName)
                .containsExactly("P001", "Keyboard", 49.99d, "EUR", 100, 5, null, null);
    }

    @Test
    void shouldPublishToMasterProductTopic() {
        productInput.pipeInput("P001", product());
        assertThat(productMasterTopic.isEmpty()).isFalse();
    }

    @Test
    void shouldPublishMetricsWhenProductMasterCreated() {
        productInput.pipeInput("P001", product());
        assertThat(productMasterTopic.isEmpty()).isFalse();
        verify(metricsService, atLeastOnce()).startTimer();
        verify(metricsService, atLeastOnce()).stopProductMasterTimer(sample);
        verify(metricsService, atLeastOnce()).incrementProductMaster();
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    private Product product() {
        return Product.newBuilder()
                .setProductId("P001")
                .setSku("KB001")
                .setName("Keyboard")
                .setBrand("Microsoft")
                .setCategory("Accessories")
                .setActive(true)
                .setAlertThreshold(50)
                .setCreatedAt(Instant.parse("2026-08-29T10:00:00Z"))
                .build();
    }

    private Price price() {
        return Price.newBuilder()
                .setProductId("P001")
                .setAmount(49.99d)
                .setCurrency("EUR")
                .setLastUpdatedAt(Instant.parse("2026-08-31T10:00:00Z"))
                .build();
    }

    private Supplier supplier() {
        return Supplier.newBuilder()
                .setProductId("P001")
                .setSupplierId("SUP001")
                .setSupplierName("TechSupplier")
                .setSupplierCountry("FR")
                .setLastUpdatedAt(Instant.parse("2026-08-31T10:01:00Z"))
                .build();
    }

    private Stock stock(int available, int reserved) {
        return Stock.newBuilder()
                .setProductId("P001")
                .setAvailableQuantity(available)
                .setReservedQuantity(reserved)
                .setWarehouseCode("WH001")
                .setLastUpdatedAt(Instant.parse("2026-08-31T10:02:00Z"))
                .build();
    }

    private Marketing marketing() {
        return Marketing.newBuilder()
                .setProductId("P001")
                .setShortDescription("Mechanical keyboard")
                .setLongDescription("Premium keyboard")
                .setTags(List.of("keyboard", "gaming"))
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
