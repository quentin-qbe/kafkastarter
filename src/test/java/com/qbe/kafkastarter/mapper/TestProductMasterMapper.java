package com.qbe.kafkastarter.mapper;

import static org.junit.jupiter.api.Assertions.*;

import com.qbe.avro.ProductMaster;
import com.qbe.kafkastarter.dto.ProductMasterDto;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.mapstruct.factory.Mappers;

class TestProductMasterMapper {

    private final ProductMasterMapper mapper = Mappers.getMapper(ProductMasterMapper.class);

    @Test
    void shouldMapDtoToAvro() {
        Instant createdAt = Instant.parse("2026-08-26T09:00:00Z");

        Instant updatedAt = Instant.parse("2026-08-26T09:04:00Z");

        ProductMasterDto dto = buildDto(createdAt, updatedAt);

        ProductMaster event = mapper.toAvro(dto);

        assertAll(
                () -> assertEquals("P001", event.getProductId()),
                () -> assertEquals("KB001", event.getSku()),
                () -> assertEquals("Keyboard", event.getName()),
                () -> assertEquals("Microsoft", event.getBrand()),
                () -> assertEquals("Accessories", event.getCategory()),
                () -> assertTrue(event.getActive()),
                () -> assertEquals(49.99, event.getPrice()),
                () -> assertEquals("EUR", event.getCurrency()),
                () -> assertEquals(120, event.getAvailableStock()),
                () -> assertEquals(10, event.getReservedStock()),
                () -> assertEquals("SUP001", event.getSupplierId()),
                () -> assertEquals("TechSupplier", event.getSupplierName()),
                () -> assertEquals("France", event.getSupplierCountry()),
                () -> assertEquals("Wireless Keyboard", event.getShortDescription()),
                () -> assertEquals("Ergonomic wireless keyboard", event.getLongDescription()),
                () -> assertEquals(List.of("office", "wireless"), event.getTags()),
                () -> assertEquals(50, event.getAlertThreshold()),
                () -> assertEquals(createdAt, event.getCreatedAt()),
                () -> assertEquals(updatedAt, event.getLastUpdatedAt()));
    }

    @Test
    void shouldMapAvroToDto() {
        Instant createdAt = Instant.parse("2026-08-26T09:00:00Z");
        Instant updatedAt = Instant.parse("2026-08-26T09:04:00Z");

        ProductMaster event = ProductMaster.newBuilder()
                .setProductId("P001")
                .setSku("KB001")
                .setName("Keyboard")
                .setBrand("Microsoft")
                .setCategory("Accessories")
                .setActive(true)
                .setPrice(49.99)
                .setCurrency("EUR")
                .setAvailableStock(120)
                .setReservedStock(10)
                .setSupplierId("SUP001")
                .setSupplierName("TechSupplier")
                .setSupplierCountry("France")
                .setShortDescription("Wireless Keyboard")
                .setLongDescription("Ergonomic wireless keyboard")
                .setTags(List.of("office", "wireless"))
                .setAlertThreshold(50)
                .setCreatedAt(createdAt)
                .setLastUpdatedAt(updatedAt)
                .build();

        ProductMasterDto dto = mapper.fromAvro(event);

        assertAll(
                () -> assertEquals("P001", dto.productId()),
                () -> assertEquals("KB001", dto.sku()),
                () -> assertEquals("Keyboard", dto.name()),
                () -> assertEquals("Microsoft", dto.brand()),
                () -> assertEquals("Accessories", dto.category()),
                () -> assertTrue(dto.active()),
                () -> assertEquals(BigDecimal.valueOf(49.99), dto.price()),
                () -> assertEquals("EUR", dto.currency()),
                () -> assertEquals(120, dto.availableStock()),
                () -> assertEquals(10, dto.reservedStock()),
                () -> assertEquals("SUP001", dto.supplierId()),
                () -> assertEquals("TechSupplier", dto.supplierName()),
                () -> assertEquals("France", dto.supplierCountry()),
                () -> assertEquals("Wireless Keyboard", dto.shortDescription()),
                () -> assertEquals("Ergonomic wireless keyboard", dto.longDescription()),
                () -> assertEquals(List.of("office", "wireless"), dto.tags()),
                () -> assertEquals(createdAt, dto.createdAt()),
                () -> assertEquals(updatedAt, dto.lastUpdatedAt()));
    }

    @Test
    void shouldMapDtoToAvroAndBack() {
        Instant createdAt = Instant.parse("2026-08-26T09:00:00Z");
        Instant updatedAt = Instant.parse("2026-08-26T09:04:00Z");

        ProductMasterDto source = buildDto(createdAt, updatedAt);
        ProductMasterDto result = mapper.fromAvro(mapper.toAvro(source));

        assertEquals(source, result);
    }

    private ProductMasterDto buildDto(Instant createdAt, Instant updatedAt) {
        return ProductMasterDto.builder()
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
                .alertThreshold(50)
                .createdAt(createdAt)
                .lastUpdatedAt(updatedAt)
                .build();
    }

    @Test
    void shouldReturnNullWhenDtoIsNull() {
        ProductMaster event = mapper.toAvro(null);
        assertNull(event);
    }

    @Test
    void shouldReturnNullWhenEventIsNull() {
        ProductMasterDto dto = mapper.fromAvro(null);
        assertNull(dto);
    }
}
