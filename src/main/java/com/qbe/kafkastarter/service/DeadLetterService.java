package com.qbe.kafkastarter.service;

import com.qbe.avro.StockAlertAction;
import com.qbe.kafkastarter.constants.TopicsConstants;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class DeadLetterService {

    private final KafkaTemplate<String, StockAlertAction> kafkaTemplate;

    public void publish(StockAlertAction event) {
        kafkaTemplate.send(TopicsConstants.TOPIC_STOCK_ALERT_ACTIONS_DEAD_LETTER_QUEUE, event.getProductId(), event);
        log.error("Event sent to DLT. eventId={}, productId={}", event.getEventId(), event.getProductId());
    }
}
