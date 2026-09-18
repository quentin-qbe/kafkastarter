package com.qbe.kafkastarter.dto;

import jakarta.validation.constraints.*;
import java.time.Instant;
import lombok.Builder;

@Builder(toBuilder = true)
public record StockDto(
        @NotBlank(message = "productId is mandatory")
                @Pattern(regexp = "^P\\d+$", message = "productId must follow format P001")
                String productId,
        @NotNull(message = "availableQuantity is mandatory")
                @PositiveOrZero(message = "availableQuantity must be greater than or equal to 0")
                Integer availableQuantity,
        @NotNull(message = "reservedQuantity is mandatory")
                @PositiveOrZero(message = "reservedQuantity must be greater than or equal to 0")
                Integer reservedQuantity,
        @Pattern(regexp = "^[A-Z0-9_-]+$", message = "warehouseCode contains invalid characters")
                @NotBlank(message = "warehouseCode is mandatory")
                @Size(max = 50, message = "warehouseCode must not exceed 50 characters")
                String warehouseCode,
        @NotNull(message = "lastUpdatedAt is mandatory")
                @PastOrPresent(message = "lastUpdatedAt must be in the past or present")
                Instant lastUpdatedAt) {}
