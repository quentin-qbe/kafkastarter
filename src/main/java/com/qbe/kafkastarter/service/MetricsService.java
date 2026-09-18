package com.qbe.kafkastarter.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PostConstruct;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class MetricsService {

    private final MeterRegistry registry;

    private Counter productMasterCounter;
    private Counter stockAlertActionSuccessCounter;
    private Counter stockAlertActionFailureCounter;
    private Counter stockAlertActionDeadLetterQueueCounter;
    private Timer stockAlertActionTimer;
    private Timer productMasterTimer;
    private Timer stockAlertTimer;
    private final Map<String, Counter> stockAlertSeverityCounters = new ConcurrentHashMap<>();

    @PostConstruct
    void init() {
        productMasterCounter =
                Counter.builder("kafkastarter.productmaster.created").register(registry);

        stockAlertActionSuccessCounter = Counter.builder("kafkastarter.stockalertaction.creation.success")
                .register(registry);

        stockAlertActionFailureCounter =
                Counter.builder("kafkastarter.stockalertaction.creation.failed").register(registry);

        stockAlertActionDeadLetterQueueCounter = Counter.builder("kafkastarter.stockalertaction.dead.letter.queue")
                .description("Nombre d'événements envoyés dans la DLQ")
                .register(registry);

        stockAlertActionTimer = Timer.builder("kafkastarter.stockalertaction.pipeline")
                .description("Temps de traitement d'une action sur alerte")
                .publishPercentiles(0.50, 0.95, 0.99)
                .register(registry);

        productMasterTimer = Timer.builder("kafkastarter.productmaster.pipeline")
                .description("Temps de traitement d'une création productMaster")
                .publishPercentiles(0.50, 0.95, 0.99)
                .register(registry);

        stockAlertTimer = Timer.builder("kafkastarter.stockalert.pipeline")
                .description("Temps de traitement d'une alerte sur le stock")
                .publishPercentiles(0.50, 0.95, 0.99)
                .register(registry);
    }

    public void incrementProductMaster() {
        productMasterCounter.increment();
    }

    public void incrementStockAlert(String severity) {
        stockAlertSeverityCounters
                .computeIfAbsent(severity, s -> Counter.builder("kafkastarter.stockalert.created")
                        .tag("severity", s)
                        .register(registry))
                .increment();
    }

    public void incrementStockAlertSuccessAction() {
        stockAlertActionSuccessCounter.increment();
    }

    public void incrementStockAlertFailureAction() {
        stockAlertActionFailureCounter.increment();
    }

    public Timer.Sample startTimer() {
        return Timer.start(registry);
    }

    public void stopProductMasterTimer(Timer.Sample sample) {
        sample.stop(productMasterTimer);
    }

    public void stopStockAlertTimer(Timer.Sample sample) {
        sample.stop(stockAlertTimer);
    }

    public void stopStockAlertActionTimer(Timer.Sample sample) {
        sample.stop(stockAlertActionTimer);
    }

    public void incrementStockAlertActionDeadLetterQueue() {
        stockAlertActionDeadLetterQueueCounter.increment();
    }
}
