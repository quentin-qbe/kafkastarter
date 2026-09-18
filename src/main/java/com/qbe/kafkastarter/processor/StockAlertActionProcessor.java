package com.qbe.kafkastarter.processor;

import com.qbe.avro.StockAlertAction;
import com.qbe.kafkastarter.entity.StockAlertActionEntity;
import com.qbe.kafkastarter.service.DeadLetterService;
import com.qbe.kafkastarter.service.MetricsService;
import com.qbe.kafkastarter.service.StockAlertActionService;
import io.micrometer.core.instrument.Timer;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.ContextualFixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;

@RequiredArgsConstructor
@Slf4j
public class StockAlertActionProcessor extends ContextualFixedKeyProcessor<String, StockAlertAction, Void> {

    private final StockAlertActionService stockAlertActionService;
    private final MetricsService metricsService;
    private final DeadLetterService deadLetterService;

    @Override
    public void process(FixedKeyRecord<String, StockAlertAction> record) {

        if (record.value() == null) {
            return;
        }

        Timer.Sample sample = metricsService.startTimer();

        StockAlertAction event = record.value();

        try {

            stockAlertActionService.save(StockAlertActionEntity.builder()
                    .eventId(event.getEventId())
                    .productId(event.getProductId())
                    .action(mapAction(event.getAction()))
                    .username(event.getUser())
                    .processedAt(Instant.parse(event.getProcessedAt()))
                    .createdAt(Instant.now())
                    .build());

            log.info("Sauvegarde en BDD effectuee pour productId={}", event.getProductId());
        } catch (Exception e) {
            log.error("Erreur lors du traitement de l'evenement eventId={}", event.getEventId(), e);
            deadLetterService.publish(event);
            metricsService.incrementStockAlertActionDeadLetterQueue();
        } finally {
            metricsService.stopStockAlertActionTimer(sample);
        }
    }

    private com.qbe.kafkastarter.enums.AlertActionType mapAction(com.qbe.avro.AlertActionType action) {
        return com.qbe.kafkastarter.enums.AlertActionType.valueOf(action.name());
    }
}
