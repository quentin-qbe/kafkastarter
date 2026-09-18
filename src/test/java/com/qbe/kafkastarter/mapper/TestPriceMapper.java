package com.qbe.kafkastarter.mapper;

import static org.junit.jupiter.api.Assertions.*;

import com.qbe.avro.Price;
import com.qbe.kafkastarter.dto.PriceDto;
import java.math.BigDecimal;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.mapstruct.factory.Mappers;

class TestPriceMapper {

    private final PriceMapper mapper = Mappers.getMapper(PriceMapper.class);

    @Test
    void shouldMapDtoToAvro() {
        Instant updatedAt = Instant.parse("2026-08-26T09:04:00Z");

        PriceDto dto = PriceDto.builder()
                .productId("P001")
                .amount(BigDecimal.valueOf(49.99))
                .currency("EUR")
                .lastUpdatedAt(updatedAt)
                .build();

        Price event = mapper.toAvro(dto);

        assertAll(
                () -> assertEquals("P001", event.getProductId()),
                () -> assertEquals(49.99, event.getAmount()),
                () -> assertEquals("EUR", event.getCurrency()),
                () -> assertEquals(updatedAt, event.getLastUpdatedAt()));
    }

    @Test
    void shouldMapAvroToDto() {
        Instant updatedAt = Instant.parse("2026-08-26T09:04:00Z");

        Price event = Price.newBuilder()
                .setProductId("P001")
                .setAmount(49.99)
                .setCurrency("EUR")
                .setLastUpdatedAt(updatedAt)
                .build();

        PriceDto dto = mapper.fromAvro(event);

        assertAll(
                () -> assertEquals("P001", dto.productId()),
                () -> assertEquals(BigDecimal.valueOf(49.99), dto.amount()),
                () -> assertEquals("EUR", dto.currency()),
                () -> assertEquals(updatedAt, dto.lastUpdatedAt()));
    }

    @Test
    void shouldMapDtoToAvroAndBack() {
        Instant updatedAt = Instant.parse("2026-08-26T09:04:00Z");

        PriceDto source = PriceDto.builder()
                .productId("P001")
                .amount(BigDecimal.valueOf(49.99))
                .currency("EUR")
                .lastUpdatedAt(updatedAt)
                .build();

        PriceDto result = mapper.fromAvro(mapper.toAvro(source));

        assertEquals(source, result);
    }

    @Test
    void shouldReturnNullWhenDtoIsNull() {
        Price event = mapper.toAvro(null);
        assertNull(event);
    }

    @Test
    void shouldReturnNullWhenEventIsNull() {
        PriceDto dto = mapper.fromAvro(null);
        assertNull(dto);
    }
}
