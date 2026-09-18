package com.qbe.kafkastarter.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PastOrPresent;
import java.time.Instant;
import java.util.List;
import lombok.Builder;

@Builder(toBuilder = true)
public record MarketingDto(
        @NotBlank(message = "productId is mandatory") String productId,
        @NotBlank(message = "shortDescription is mandatory") String shortDescription,
        @NotBlank(message = "longDescription is mandatory") String longDescription,
        @NotNull(message = "tags is mandatory") @NotEmpty(message = "At least one tag is required")
                List<@NotBlank(message = "Tag cannot be blank") String> tags,
        @NotNull(message = "lastUpdatedAt is mandatory")
                @PastOrPresent(message = "lastUpdatedAt must be in the past or present")
                Instant lastUpdatedAt) {}
