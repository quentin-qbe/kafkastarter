package com.qbe.kafkastarter.topology;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.qbe.avro.AlertActionType;
import com.qbe.avro.StockAlertAction;
import com.qbe.kafkastarter.constants.TopicsConstants;
import com.qbe.kafkastarter.service.DeadLetterService;
import com.qbe.kafkastarter.service.MetricsService;
import com.qbe.kafkastarter.service.StockAlertActionService;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.micrometer.core.instrument.Timer;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TestStockAlertActionTopology {

    private TopologyTestDriver testDriver;

    @Mock
    private StockAlertActionService stockAlertActionService;

    private TestInputTopic<String, StockAlertAction> actionInput;

    @Mock
    private MetricsService metricsService;

    @Mock
    private Timer.Sample sample;

    @Mock
    private DeadLetterService deadLetterService;

    @BeforeEach
    void setup() {
        SpecificAvroSerde<StockAlertAction> actionSerde = buildSerde();

        StockAlertActionTopology topology =
                new StockAlertActionTopology(actionSerde, stockAlertActionService, deadLetterService, metricsService);

        StreamsBuilder builder = new StreamsBuilder();
        topology.build(builder);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-action-topology");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

        testDriver = new TopologyTestDriver(builder.build(), props);

        actionInput = testDriver.createInputTopic(
                TopicsConstants.TOPIC_STOCK_ALERT_ACTIONS, new StringSerializer(), actionSerde.serializer());

        lenient().when(metricsService.startTimer()).thenReturn(sample);
    }

    @Test
    void shouldCallServiceWhenActionReceived() {
        StockAlertAction event = StockAlertAction.newBuilder()
                .setEventId("Event-001")
                .setProductId("P001")
                .setAction(AlertActionType.ACKNOWLEDGE)
                .setUser("user")
                .setProcessedAt("2026-09-01T15:30:00Z")
                .build();

        actionInput.pipeInput("P001", event);
        Mockito.verify(stockAlertActionService).save(Mockito.any());
        Mockito.verify(metricsService).startTimer();
        Mockito.verify(metricsService).stopStockAlertActionTimer(sample);
    }

    @Test
    void shouldNotCallServiceWhenValueIsNull() {
        actionInput.pipeInput("P001", null);
        verify(stockAlertActionService, never()).save(any());
    }

    @Test
    void shouldSendToDltWhenServiceFails() {
        doThrow(new RuntimeException("DB Error")).when(stockAlertActionService).save(any());

        StockAlertAction event = StockAlertAction.newBuilder()
                .setEventId("event-001")
                .setProductId("P001")
                .setAction(AlertActionType.ACKNOWLEDGE)
                .setUser("user")
                .setProcessedAt("2026-09-01T15:30:00Z")
                .build();

        actionInput.pipeInput("P001", event);

        verify(deadLetterService).publish(event);
        verify(metricsService).incrementStockAlertActionDeadLetterQueue();
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    private <T extends SpecificRecord> SpecificAvroSerde<T> buildSerde() {
        Map<String, String> serdeConfig = Map.of("schema.registry.url", "mock://test");

        SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();

        serde.configure(serdeConfig, false);

        return serde;
    }
}
