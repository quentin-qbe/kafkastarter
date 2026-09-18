package com.qbe.kafkastarter.testcontainers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import com.qbe.avro.AlertActionType;
import com.qbe.avro.StockAlertAction;
import com.qbe.kafkastarter.TestcontainersConfiguration;
import com.qbe.kafkastarter.constants.TopicsConstants;
import com.qbe.kafkastarter.entity.StockAlertActionEntity;
import com.qbe.kafkastarter.service.StockAlertActionService;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.shaded.org.awaitility.Awaitility;

@SpringBootTest(properties = {"spring.kafka.streams.application-id=test-${random.uuid}"})
@Import(TestcontainersConfiguration.class)
class TestStockAlertActionTopologyIT {

    @Autowired
    private KafkaContainer kafkaContainer;

    @Autowired
    private StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    @Autowired
    private KafkaProperties kafkaProperties;

    @MockitoBean
    private StockAlertActionService stockAlertActionService;

    private Producer<String, StockAlertAction> producer() {
        Map<String, Object> props = new HashMap<>(kafkaProperties.buildProducerProperties());
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        return new KafkaProducer<>(props);
    }

    @Test
    void shouldProcessStockAlertAction() {
        KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();

        Awaitility.await().until(() -> kafkaStreams.state() == KafkaStreams.State.RUNNING);

        StockAlertAction action = StockAlertAction.newBuilder()
                .setEventId("Event-001")
                .setProductId("P001")
                .setAction(AlertActionType.ACKNOWLEDGE)
                .setUser("user")
                .setProcessedAt(Instant.now().toString())
                .build();

        Producer<String, StockAlertAction> producer = producer();
        producer.send(new ProducerRecord<>(TopicsConstants.TOPIC_STOCK_ALERT_ACTIONS, "P001", action));
        producer.flush();

        ArgumentCaptor<StockAlertActionEntity> captor = ArgumentCaptor.forClass(StockAlertActionEntity.class);
        verify(stockAlertActionService, timeout(10_000)).save(captor.capture());

        StockAlertActionEntity entity = captor.getValue();
        assertThat(entity.getProductId()).isEqualTo("P001");
        assertThat(entity.getUsername()).isEqualTo("user");
        assertThat(entity.getAction()).isEqualTo(com.qbe.kafkastarter.enums.AlertActionType.ACKNOWLEDGE);
        producer.close();
    }
}
