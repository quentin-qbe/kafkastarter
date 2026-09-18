package com.qbe.kafkastarter.mapper;

import static org.junit.jupiter.api.Assertions.*;

import com.qbe.avro.Product;
import com.qbe.kafkastarter.dto.ProductDto;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.mapstruct.factory.Mappers;

class TestProductMapper {

    private final ProductMapper mapper = Mappers.getMapper(ProductMapper.class);

    @Test
    void shouldMapDtoToAvro() {
        Instant createdAt = Instant.parse("2026-08-26T09:00:00Z");

        ProductDto dto = ProductDto.builder()
                .productId("P001")
                .sku("KB001")
                .name("Keyboard")
                .category("Accessories")
                .brand("Microsoft")
                .active(true)
                .alertThreshold(40)
                .createdAt(createdAt)
                .build();

        Product event = mapper.toAvro(dto);

        assertAll(
                () -> assertEquals("P001", event.getProductId()),
                () -> assertEquals("KB001", event.getSku()),
                () -> assertEquals("Keyboard", event.getName()),
                () -> assertEquals("Accessories", event.getCategory()),
                () -> assertEquals("Microsoft", event.getBrand()),
                () -> assertEquals(40, event.getAlertThreshold()),
                () -> assertTrue(event.getActive()),
                () -> assertEquals(createdAt, event.getCreatedAt()));
    }

    @Test
    void shouldMapAvroToDto() {
        Instant createdAt = Instant.parse("2026-08-26T09:00:00Z");

        Product event = Product.newBuilder()
                .setProductId("P001")
                .setSku("KB001")
                .setName("Keyboard")
                .setCategory("Accessories")
                .setBrand("Microsoft")
                .setActive(true)
                .setAlertThreshold(40)
                .setCreatedAt(createdAt)
                .build();

        ProductDto dto = mapper.fromAvro(event);

        assertAll(
                () -> assertEquals("P001", dto.productId()),
                () -> assertEquals("KB001", dto.sku()),
                () -> assertEquals("Keyboard", dto.name()),
                () -> assertEquals("Accessories", dto.category()),
                () -> assertEquals("Microsoft", dto.brand()),
                () -> assertEquals(40, dto.alertThreshold()),
                () -> assertTrue(dto.active()),
                () -> assertEquals(createdAt, dto.createdAt()));
    }

    @Test
    void shouldMapDtoToAvroAndBack() {
        Instant createdAt = Instant.parse("2026-08-26T09:00:00Z");

        ProductDto source = ProductDto.builder()
                .productId("P001")
                .sku("KB001")
                .name("Keyboard")
                .category("Accessories")
                .brand("Microsoft")
                .active(true)
                .alertThreshold(40)
                .createdAt(createdAt)
                .build();

        ProductDto result = mapper.fromAvro(mapper.toAvro(source));
        assertEquals(source, result);
    }

    @Test
    void shouldReturnNullWhenDtoIsNull() {
        Product event = mapper.toAvro(null);
        assertNull(event);
    }

    @Test
    void shouldReturnNullWhenEventIsNull() {
        ProductDto dto = mapper.fromAvro(null);
        assertNull(dto);
    }
}
