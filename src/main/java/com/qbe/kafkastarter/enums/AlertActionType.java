package com.qbe.kafkastarter.enums;

import lombok.Getter;

@Getter
public enum AlertActionType {
    ACKNOWLEDGE("ACKNOWLEDGE"),
    UNACKNOWLEDGE("UNACKNOWLEDGE"),
    RESOLVE("RESOLVE"),
    REOPEN("REOPEN");

    private final String label;

    AlertActionType(String label) {
        this.label = label;
    }
}
