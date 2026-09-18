package com.qbe.kafkastarter.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PastOrPresent;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import java.time.Instant;
import lombok.Builder;

@Builder(toBuilder = true)
public record SupplierDto(
        @NotBlank(message = "productId is mandatory")
                @Pattern(regexp = "^P\\d{3}$", message = "productId must follow format P001")
                String productId,
        @NotBlank(message = "supplierId is mandatory")
                @Pattern(regexp = "^SUP\\d{3}$", message = "supplierId must follow format SUP001")
                String supplierId,
        @NotBlank(message = "supplierName is mandatory")
                @Size(max = 255, message = "supplierName must not exceed 255 characters")
                String supplierName,
        @NotBlank(message = "supplierCountry is mandatory")
                @Size(max = 100, message = "supplierCountry must not exceed 100 characters")
                String supplierCountry,
        @NotNull(message = "lastUpdatedAt is mandatory")
                @PastOrPresent(message = "lastUpdatedAt must be in the past or present")
                Instant lastUpdatedAt) {}
