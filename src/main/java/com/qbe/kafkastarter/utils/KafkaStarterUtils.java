package com.qbe.kafkastarter.utils;

import com.qbe.avro.ProductMaster;
import com.qbe.kafkastarter.enums.StockAlertSeverity;

public class KafkaStarterUtils {

    private KafkaStarterUtils() {}

    public static StockAlertSeverity computeSeverity(ProductMaster product) {
        if (product.getAvailableStock() == 0) {
            return StockAlertSeverity.OUT_OF_STOCK;
        }
        if (product.getAvailableStock() <= 5) {
            return StockAlertSeverity.CRITICAL;
        }
        return StockAlertSeverity.LOW;
    }
}
