package com.qbe.kafkastarter.enums;

import lombok.Getter;

@Getter
public enum StockAlertSeverity {
    LOW("LOW"),
    CRITICAL("CRITICAL"),
    OUT_OF_STOCK("OUT_OF_STOCK");

    private final String label;

    StockAlertSeverity(String label) {
        this.label = label;
    }
}
