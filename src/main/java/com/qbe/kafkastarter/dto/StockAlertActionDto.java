package com.qbe.kafkastarter.dto;

import com.qbe.kafkastarter.enums.AlertActionType;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PastOrPresent;
import java.time.Instant;

public record StockAlertActionDto(
        @NotBlank(message = "eventId is mandatory") String eventId,
        @NotBlank(message = "productId is mandatory") String productId,
        @NotNull(message = "action is mandatory") AlertActionType action,
        @NotBlank(message = "user is mandatory") String user,
        @NotNull(message = "processedAt is mandatory") @PastOrPresent(message = "processedAt cannot be in the future")
                Instant processedAt) {}
