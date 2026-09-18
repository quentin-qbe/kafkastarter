package com.qbe.kafkastarter.mapper;

import static org.junit.jupiter.api.Assertions.*;

import com.qbe.avro.Stock;
import com.qbe.kafkastarter.dto.StockDto;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.mapstruct.factory.Mappers;

class TestStockMapper {

    private final StockMapper mapper = Mappers.getMapper(StockMapper.class);

    @Test
    void shouldMapDtoToAvro() {
        Instant updatedAt = Instant.parse("2026-08-26T09:04:00Z");

        StockDto dto = StockDto.builder()
                .productId("P001")
                .availableQuantity(120)
                .reservedQuantity(10)
                .warehouseCode("WH_PARIS")
                .lastUpdatedAt(updatedAt)
                .build();

        Stock event = mapper.toAvro(dto);

        assertAll(
                () -> assertEquals("P001", event.getProductId()),
                () -> assertEquals(120, event.getAvailableQuantity()),
                () -> assertEquals(10, event.getReservedQuantity()),
                () -> assertEquals("WH_PARIS", event.getWarehouseCode()),
                () -> assertEquals(updatedAt, event.getLastUpdatedAt()));
    }

    @Test
    void shouldMapAvroToDto() {
        Instant updatedAt = Instant.parse("2026-08-26T09:04:00Z");

        Stock event = Stock.newBuilder()
                .setProductId("P001")
                .setAvailableQuantity(120)
                .setReservedQuantity(10)
                .setWarehouseCode("WH_PARIS")
                .setLastUpdatedAt(updatedAt)
                .build();

        StockDto dto = mapper.fromAvro(event);

        assertAll(
                () -> assertEquals("P001", dto.productId()),
                () -> assertEquals(120, dto.availableQuantity()),
                () -> assertEquals(10, dto.reservedQuantity()),
                () -> assertEquals("WH_PARIS", dto.warehouseCode()),
                () -> assertEquals(updatedAt, dto.lastUpdatedAt()));
    }

    @Test
    void shouldMapDtoToAvroAndBack() {
        Instant updatedAt = Instant.parse("2026-08-26T09:04:00Z");

        StockDto source = StockDto.builder()
                .productId("P001")
                .availableQuantity(120)
                .reservedQuantity(10)
                .warehouseCode("WH_PARIS")
                .lastUpdatedAt(updatedAt)
                .build();

        StockDto result = mapper.fromAvro(mapper.toAvro(source));

        assertEquals(source, result);
    }

    @Test
    void shouldReturnNullWhenDtoIsNull() {
        Stock event = mapper.toAvro(null);
        assertNull(event);
    }

    @Test
    void shouldReturnNullWhenEventIsNull() {
        StockDto dto = mapper.fromAvro(null);
        assertNull(dto);
    }
}
