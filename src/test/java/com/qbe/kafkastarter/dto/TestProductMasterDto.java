package com.qbe.kafkastarter.dto;

import static org.junit.jupiter.api.Assertions.*;

import java.io.*;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class TestProductMasterDto {

    @Test
    void shouldBuildProductMasterDto() {
        Instant createdAt = Instant.parse("2026-08-26T09:00:00Z");
        Instant updatedAt = Instant.parse("2026-08-26T09:04:00Z");

        ProductMasterDto dto = ProductMasterDto.builder()
                .productId("P001")
                .sku("KB001")
                .name("Keyboard")
                .brand("Microsoft")
                .category("Accessories")
                .active(true)
                .price(BigDecimal.valueOf(49.99))
                .currency("EUR")
                .availableStock(120)
                .reservedStock(10)
                .supplierId("SUP001")
                .supplierName("TechSupplier")
                .supplierCountry("France")
                .shortDescription("Wireless Keyboard")
                .longDescription("Ergonomic wireless keyboard")
                .tags(List.of("office", "wireless"))
                .createdAt(createdAt)
                .lastUpdatedAt(updatedAt)
                .build();

        assertEquals("P001", dto.productId());
        assertEquals("KB001", dto.sku());
        assertEquals("Keyboard", dto.name());
        assertEquals("Microsoft", dto.brand());
        assertEquals("Accessories", dto.category());

        assertTrue(dto.active());

        assertEquals(BigDecimal.valueOf(49.99), dto.price());
        assertEquals("EUR", dto.currency());

        assertEquals(120, dto.availableStock());
        assertEquals(10, dto.reservedStock());

        assertEquals("SUP001", dto.supplierId());
        assertEquals("TechSupplier", dto.supplierName());
        assertEquals("France", dto.supplierCountry());

        assertEquals("Wireless Keyboard", dto.shortDescription());
        assertEquals("Ergonomic wireless keyboard", dto.longDescription());

        assertEquals(List.of("office", "wireless"), dto.tags());

        assertEquals(createdAt, dto.createdAt());
        assertEquals(updatedAt, dto.lastUpdatedAt());
    }

    @Test
    void shouldSupportToBuilder() {

        ProductMasterDto original =
                ProductMasterDto.builder().productId("P001").name("Keyboard").build();

        ProductMasterDto updated =
                original.toBuilder().name("Mechanical Keyboard").build();

        assertEquals("P001", updated.productId());
        assertEquals("Mechanical Keyboard", updated.name());

        assertEquals("Keyboard", original.name());
    }

    @Test
    void shouldBeSerializable() throws Exception {

        ProductMasterDto dto =
                ProductMasterDto.builder().productId("P001").name("Keyboard").build();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(dto);
        }

        byte[] serialized = baos.toByteArray();

        ProductMasterDto deserialized;

        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(serialized))) {

            deserialized = (ProductMasterDto) ois.readObject();
        }

        assertEquals(dto, deserialized);
    }
}
