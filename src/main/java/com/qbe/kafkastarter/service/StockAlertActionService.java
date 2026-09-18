package com.qbe.kafkastarter.service;

import com.qbe.kafkastarter.entity.StockAlertActionEntity;
import com.qbe.kafkastarter.repository.StockAlertActionRepository;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@Transactional
@RequiredArgsConstructor
public class StockAlertActionService {

    private final StockAlertActionRepository stockAlertActionRepository;
    private final MetricsService metricsService;

    public void save(StockAlertActionEntity entity) {
        try {
            if (stockAlertActionRepository.existsByEventId(entity.getEventId())) {
                log.info("Duplicate event ignored. eventId={}", entity.getEventId());
                return;
            }
            stockAlertActionRepository.save(entity);
            metricsService.incrementStockAlertSuccessAction();
            log.info("Event with productId={} and eventId={} saved.", entity.getProductId(), entity.getEventId());
        } catch (Exception e) {
            log.error(
                    "Failed to save event with productId={} and eventId={}",
                    entity.getProductId(),
                    entity.getEventId(),
                    e);
            metricsService.incrementStockAlertFailureAction();
            throw e;
        }
    }
}
