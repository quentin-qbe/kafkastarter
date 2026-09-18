package com.qbe.kafkastarter.testcontainers;

import static org.assertj.core.api.Assertions.assertThat;

import com.qbe.avro.ProductMaster;
import com.qbe.avro.StockAlert;
import com.qbe.kafkastarter.TestcontainersConfiguration;
import com.qbe.kafkastarter.constants.StateStoreConstants;
import com.qbe.kafkastarter.constants.TopicsConstants;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.shaded.org.awaitility.Awaitility;

@SpringBootTest(properties = {"spring.kafka.streams.application-id=test-${random.uuid}"})
@Import(TestcontainersConfiguration.class)
class TestStockAlertTopologyIT {

    @Autowired
    private StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    @Autowired
    private KafkaContainer kafkaContainer;

    @Autowired
    private KafkaProperties kafkaProperties;

    private Producer<String, ProductMaster> producer() {
        Map<String, Object> props = new HashMap<>(kafkaProperties.buildProducerProperties());
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        return new KafkaProducer<>(props);
    }

    private Consumer<String, StockAlert> consumer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        props.put("schema.registry.url", "mock://test");
        return new KafkaConsumer<>(props);
    }

    @Test
    void shouldCreateStateStore() {
        KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();

        Awaitility.await().untilAsserted(() -> assertThat(kafkaStreams.store(StoreQueryParameters.fromNameAndType(
                        StateStoreConstants.STOCK_ALERT_STORE, QueryableStoreTypes.keyValueStore())))
                .isNotNull());
    }

    @Test
    void shouldCreateStockAlert() {
        System.out.println("PROPERTIES=" + kafkaProperties.buildProducerProperties());
        ProductMaster product = ProductMaster.newBuilder()
                .setProductId("P001")
                .setSku("KB001")
                .setName("Mechanical Keyboard")
                .setBrand("Microsoft")
                .setCategory("Accessories")
                .setActive(true)
                .setPrice(49.99)
                .setCurrency("EUR")
                .setAvailableStock(0)
                .setReservedStock(2)
                .setSupplierId("SUP001")
                .setSupplierName("TechSupplier")
                .setSupplierCountry("FR")
                .setShortDescription("Mechanical keyboard")
                .setLongDescription("High quality mechanical keyboard with RGB lighting")
                .setTags(List.of("keyboard", "gaming", "rgb"))
                .setAlertThreshold(20)
                .setCreatedAt(Instant.parse("2026-08-29T10:00:00Z"))
                .setLastUpdatedAt(Instant.parse("2026-09-02T13:00:00Z"))
                .build();

        Producer<String, ProductMaster> producer = producer();

        KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();

        Awaitility.await().until(() -> kafkaStreams.state() == KafkaStreams.State.RUNNING);

        producer.send(new ProducerRecord<>(TopicsConstants.TOPIC_MASTER_PRODUCT, "P001", product));

        producer.flush();

        Consumer<String, StockAlert> consumer = consumer();

        consumer.subscribe(List.of(TopicsConstants.TOPIC_STOCK_ALERT));

        consumer.poll(Duration.ofSeconds(1));
        consumer.seekToBeginning(consumer.assignment());

        Awaitility.await().untilAsserted(() -> {
            ConsumerRecord<String, StockAlert> record =
                    KafkaTestUtils.getSingleRecord(consumer, TopicsConstants.TOPIC_STOCK_ALERT);

            assertThat(record).isNotNull();

            StockAlert alert = record.value();

            assertThat(alert.getProductId()).isEqualTo("P001");
            assertThat(alert.getSku()).isEqualTo("KB001");
            assertThat(alert.getCurrentStock()).isEqualTo(0);
            assertThat(alert.getThreshold()).isEqualTo(20);
            assertThat(alert.getSeverity()).isEqualTo("OUT_OF_STOCK");
        });

        producer.close();
        consumer.close();
    }
}
