package com.qbe.kafkastarter.dto;

import jakarta.validation.constraints.*;
import java.math.BigDecimal;
import java.time.Instant;
import lombok.Builder;

@Builder(toBuilder = true)
public record PriceDto(
        @NotBlank(message = "productId is mandatory")
                @Pattern(regexp = "^P\\d+$", message = "productId must follow format P001")
                String productId,
        @NotNull(message = "amount is mandatory")
                @DecimalMin(value = "0.0", inclusive = false, message = "amount must be greater than 0")
                BigDecimal amount,
        @NotBlank(message = "currency is mandatory")
                @Pattern(regexp = "^[A-Z]{3}$", message = "currency must be a valid ISO code")
                String currency,
        @NotNull(message = "lastUpdatedAt is mandatory")
                @PastOrPresent(message = "lastUpdatedAt must be in the past or present")
                Instant lastUpdatedAt) {}
