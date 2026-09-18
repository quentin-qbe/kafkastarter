package com.qbe.kafkastarter.dto;

import jakarta.validation.constraints.*;
import java.time.Instant;
import lombok.Builder;

@Builder(toBuilder = true)
public record ProductDto(
        @NotBlank(message = "productId is mandatory")
                @Pattern(regexp = "^P\\d+$", message = "productId must follow format P001")
                String productId,
        @Pattern(regexp = "^[A-Z0-9-]+$", message = "sku contains invalid characters")
                @NotBlank(message = "sku is mandatory")
                @Size(max = 50, message = "sku must not exceed 50 characters")
                String sku,
        @NotBlank(message = "name is mandatory") @Size(max = 255, message = "name must not exceed 255 characters")
                String name,
        @NotBlank(message = "category is mandatory")
                @Size(max = 100, message = "category must not exceed 100 characters")
                String category,
        @NotBlank(message = "brand is mandatory") @Size(max = 100, message = "brand must not exceed 100 characters")
                String brand,
        boolean active,
        @NotNull(message = "alertThreshold is mandatory")
                @Min(value = 0, message = "alertThreshold must be greater than or equal to 0")
                @Max(value = 200, message = "alertThreshold must be less than or equal to 200")
                Integer alertThreshold,
        @NotNull(message = "createdAt is mandatory")
                @PastOrPresent(message = "createdAt must be in the past or present")
                Instant createdAt) {}
