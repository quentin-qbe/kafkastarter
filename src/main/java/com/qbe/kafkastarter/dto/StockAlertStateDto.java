package com.qbe.kafkastarter.dto;

import lombok.Builder;

@Builder
public record StockAlertStateDto(
        String productId, boolean alertActive, String severity, Integer lastKnownStock, String lastAlertDate) {}
