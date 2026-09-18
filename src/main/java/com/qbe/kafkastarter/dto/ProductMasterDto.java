package com.qbe.kafkastarter.dto;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import lombok.Builder;

@Builder(toBuilder = true)
public record ProductMasterDto(
        String productId,
        String sku,
        String name,
        String brand,
        String category,
        boolean active,
        BigDecimal price,
        String currency,
        Integer availableStock,
        Integer reservedStock,
        String supplierId,
        String supplierName,
        String supplierCountry,
        String shortDescription,
        String longDescription,
        @JacksonXmlElementWrapper(localName = "tags") @JacksonXmlProperty(localName = "tag") List<String> tags,
        Integer alertThreshold,
        Instant createdAt,
        Instant lastUpdatedAt)
        implements Serializable {}
