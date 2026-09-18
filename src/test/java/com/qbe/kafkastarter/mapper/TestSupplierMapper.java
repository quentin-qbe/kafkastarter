package com.qbe.kafkastarter.mapper;

import static org.junit.jupiter.api.Assertions.*;

import com.qbe.avro.Supplier;
import com.qbe.kafkastarter.dto.SupplierDto;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.mapstruct.factory.Mappers;

class TestSupplierMapper {

    private final SupplierMapper mapper = Mappers.getMapper(SupplierMapper.class);

    @Test
    void shouldMapDtoToAvro() {
        Instant updatedAt = Instant.parse("2026-08-26T09:04:00Z");

        SupplierDto dto = SupplierDto.builder()
                .productId("P001")
                .supplierId("SUP001")
                .supplierName("TechSupplier")
                .supplierCountry("France")
                .lastUpdatedAt(updatedAt)
                .build();

        Supplier event = mapper.toAvro(dto);

        assertAll(
                () -> assertEquals("P001", event.getProductId()),
                () -> assertEquals("SUP001", event.getSupplierId()),
                () -> assertEquals("TechSupplier", event.getSupplierName()),
                () -> assertEquals("France", event.getSupplierCountry()),
                () -> assertEquals(updatedAt, event.getLastUpdatedAt()));
    }

    @Test
    void shouldMapAvroToDto() {
        Instant updatedAt = Instant.parse("2026-08-26T09:04:00Z");

        Supplier event = Supplier.newBuilder()
                .setProductId("P001")
                .setSupplierId("SUP001")
                .setSupplierName("TechSupplier")
                .setSupplierCountry("France")
                .setLastUpdatedAt(updatedAt)
                .build();

        SupplierDto dto = mapper.fromAvro(event);

        assertAll(
                () -> assertEquals("P001", dto.productId()),
                () -> assertEquals("SUP001", dto.supplierId()),
                () -> assertEquals("TechSupplier", dto.supplierName()),
                () -> assertEquals("France", dto.supplierCountry()),
                () -> assertEquals(updatedAt, dto.lastUpdatedAt()));
    }

    @Test
    void shouldMapDtoToAvroAndBack() {
        Instant updatedAt = Instant.parse("2026-08-26T09:04:00Z");

        SupplierDto source = SupplierDto.builder()
                .productId("P001")
                .supplierId("SUP001")
                .supplierName("TechSupplier")
                .supplierCountry("France")
                .lastUpdatedAt(updatedAt)
                .build();

        SupplierDto result = mapper.fromAvro(mapper.toAvro(source));
        assertEquals(source, result);
    }

    @Test
    void shouldReturnNullWhenDtoIsNull() {
        Supplier event = mapper.toAvro(null);
        assertNull(event);
    }

    @Test
    void shouldReturnNullWhenEventIsNull() {
        SupplierDto dto = mapper.fromAvro(null);
        assertNull(dto);
    }
}
