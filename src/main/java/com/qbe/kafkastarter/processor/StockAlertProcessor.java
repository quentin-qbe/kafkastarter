package com.qbe.kafkastarter.processor;

import com.qbe.avro.ProductMaster;
import com.qbe.avro.StockAlert;
import com.qbe.avro.StockAlertState;
import com.qbe.kafkastarter.constants.StateStoreConstants;
import com.qbe.kafkastarter.service.MetricsService;
import com.qbe.kafkastarter.utils.KafkaStarterUtils;
import io.micrometer.core.instrument.Timer;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.processor.api.ContextualFixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;

@RequiredArgsConstructor
public class StockAlertProcessor extends ContextualFixedKeyProcessor<String, ProductMaster, StockAlert> {

    private KeyValueStore<String, StockAlertState> store;
    private final MetricsService metricsService;

    @Override
    public void init(FixedKeyProcessorContext<String, StockAlert> context) {
        super.init(context);
        store = context.getStateStore(StateStoreConstants.STOCK_ALERT_STORE);
    }

    @Override
    public void process(FixedKeyRecord<String, ProductMaster> productMasterRecord) {
        Timer.Sample sample = metricsService.startTimer();
        try {
            ProductMaster product = productMasterRecord.value();

            if (product == null || product.getAvailableStock() == null || product.getAlertThreshold() == null) {
                return;
            }

            boolean stockLow = product.getAvailableStock() < product.getAlertThreshold();

            StockAlertState state = store.get(productMasterRecord.key());

            if (state == null) {
                state = StockAlertState.newBuilder().setAlertActive(false).build();
            }

            if (stockLow && !state.getAlertActive()) {
                String severity = KafkaStarterUtils.computeSeverity(product).name();
                String alertDate = Instant.now().toString();

                StockAlert alert = StockAlert.newBuilder()
                        .setProductId(product.getProductId())
                        .setSku(product.getSku())
                        .setCurrentStock(product.getAvailableStock())
                        .setThreshold(product.getAlertThreshold())
                        .setSeverity(severity)
                        .setAlertDate(alertDate)
                        .build();

                System.out.println("FORWARDING " + productMasterRecord.key());
                context().forward(productMasterRecord.withValue(alert));

                store.put(
                        productMasterRecord.key(),
                        StockAlertState.newBuilder()
                                .setAlertActive(true)
                                .setSeverity(severity)
                                .setLastKnownStock(product.getAvailableStock())
                                .setLastAlertDate(alertDate)
                                .build());

                return;
            }

            if (!stockLow && state.getAlertActive()) {
                store.put(
                        productMasterRecord.key(),
                        StockAlertState.newBuilder()
                                .setAlertActive(false)
                                .setSeverity(state.getSeverity())
                                .setLastKnownStock(product.getAvailableStock())
                                .setLastAlertDate(state.getLastAlertDate())
                                .build());
            }
        } finally {
            metricsService.stopStockAlertTimer(sample);
        }
    }
}
