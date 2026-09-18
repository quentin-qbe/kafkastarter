package com.qbe.kafkastarter.testcontainers;

import static org.assertj.core.api.Assertions.assertThat;

import com.qbe.avro.*;
import com.qbe.kafkastarter.TestcontainersConfiguration;
import com.qbe.kafkastarter.constants.TopicsConstants;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
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
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.shaded.org.awaitility.Awaitility;

@SpringBootTest(properties = {"spring.kafka.streams.application-id=test-${random.uuid}"})
@Import(TestcontainersConfiguration.class)
class TestProductMasterTopologyIT {

    @Autowired
    private StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    @Autowired
    private KafkaContainer kafkaContainer;

    @Autowired
    private KafkaProperties kafkaProperties;

    private Producer<String, Object> producer() {
        Map<String, Object> props = new HashMap<>(kafkaProperties.buildProducerProperties());
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        return new KafkaProducer<>(props);
    }

    private Consumer<String, ProductMaster> consumer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        props.put("schema.registry.url", "mock://test");
        return new KafkaConsumer<>(props);
    }

    @Test
    void shouldBuildProductMaster() {
        KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();

        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .until(() -> kafkaStreams.state() == KafkaStreams.State.RUNNING);

        String productId = "P-" + UUID.randomUUID();
        Instant now = Instant.now();

        Consumer<String, ProductMaster> consumer = consumer();
        var partitions = consumer.partitionsFor(TopicsConstants.TOPIC_MASTER_PRODUCT);
        var topicPartitions = partitions.stream()
                .map(p -> new TopicPartition(p.topic(), p.partition()))
                .toList();

        consumer.assign(topicPartitions);
        consumer.seekToBeginning(topicPartitions);

        Producer<String, Object> producer = producer();

        producer.send(new ProducerRecord<>(
                TopicsConstants.TOPIC_PRODUCT,
                productId,
                Product.newBuilder()
                        .setProductId(productId)
                        .setSku("KB001")
                        .setName("Mechanical Keyboard")
                        .setBrand("Microsoft")
                        .setCategory("Accessories")
                        .setActive(true)
                        .setCreatedAt(now)
                        .setAlertThreshold(50)
                        .build()));

        producer.send(new ProducerRecord<>(
                TopicsConstants.TOPIC_PRICE,
                productId,
                Price.newBuilder()
                        .setProductId(productId)
                        .setAmount(49.99)
                        .setCurrency("EUR")
                        .setLastUpdatedAt(now)
                        .build()));

        producer.send(new ProducerRecord<>(
                TopicsConstants.TOPIC_STOCK,
                productId,
                Stock.newBuilder()
                        .setProductId(productId)
                        .setAvailableQuantity(10)
                        .setReservedQuantity(5)
                        .setWarehouseCode("W001")
                        .setLastUpdatedAt(now)
                        .build()));

        producer.send(new ProducerRecord<>(
                TopicsConstants.TOPIC_SUPPLIER,
                productId,
                Supplier.newBuilder()
                        .setProductId(productId)
                        .setSupplierId("SUP001")
                        .setSupplierName("TechSupplier")
                        .setSupplierCountry("FR")
                        .setLastUpdatedAt(now)
                        .build()));

        producer.send(new ProducerRecord<>(
                TopicsConstants.TOPIC_MARKETING,
                productId,
                Marketing.newBuilder()
                        .setProductId(productId)
                        .setShortDescription("Mechanical keyboard")
                        .setLongDescription("High quality mechanical keyboard with RGB lighting")
                        .setTags(List.of("keyboard", "gaming", "rgb"))
                        .setLastUpdatedAt(now)
                        .build()));

        producer.flush();

        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var records = consumer.poll(Duration.ofSeconds(5));

            ProductMaster master = null;

            for (var record : records) {
                if (productId.equals(record.key())) {
                    master = record.value();
                }
            }

            assertThat(master).isNotNull();
            assertThat(master.getProductId()).isEqualTo(productId);
            assertThat(master.getSku()).isEqualTo("KB001");
            assertThat(master.getPrice()).isEqualTo(49.99);
            assertThat(master.getAvailableStock()).isEqualTo(10);
            assertThat(master.getReservedStock()).isEqualTo(5);
            assertThat(master.getSupplierId()).isEqualTo("SUP001");
            assertThat(master.getSupplierName()).isEqualTo("TechSupplier");
        });

        producer.close();
        consumer.close();
    }
}
