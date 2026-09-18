package com.qbe.kafkastarter.mapper;

import static org.junit.jupiter.api.Assertions.*;

import com.qbe.avro.Marketing;
import com.qbe.kafkastarter.dto.MarketingDto;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.mapstruct.factory.Mappers;

class TestMarketingMapper {

    private final MarketingMapper mapper = Mappers.getMapper(MarketingMapper.class);

    @Test
    void shouldMapDtoToAvro() {
        Instant updatedAt = Instant.parse("2026-08-26T09:04:00Z");

        MarketingDto dto = MarketingDto.builder()
                .productId("P001")
                .shortDescription("Wireless Keyboard")
                .longDescription("Ergonomic wireless keyboard")
                .tags(List.of("office", "wireless"))
                .lastUpdatedAt(updatedAt)
                .build();

        Marketing event = mapper.toAvro(dto);

        assertAll(
                () -> assertEquals("P001", event.getProductId()),
                () -> assertEquals("Wireless Keyboard", event.getShortDescription()),
                () -> assertEquals("Ergonomic wireless keyboard", event.getLongDescription()),
                () -> assertEquals(List.of("office", "wireless"), event.getTags()),
                () -> assertEquals(updatedAt, event.getLastUpdatedAt()));
    }

    @Test
    void shouldMapAvroToDto() {
        Instant updatedAt = Instant.parse("2026-08-26T09:04:00Z");

        Marketing event = Marketing.newBuilder()
                .setProductId("P001")
                .setShortDescription("Wireless Keyboard")
                .setLongDescription("Ergonomic wireless keyboard")
                .setTags(List.of("office", "wireless"))
                .setLastUpdatedAt(updatedAt)
                .build();

        MarketingDto dto = mapper.fromAvro(event);

        assertAll(
                () -> assertEquals("P001", dto.productId()),
                () -> assertEquals("Wireless Keyboard", dto.shortDescription()),
                () -> assertEquals("Ergonomic wireless keyboard", dto.longDescription()),
                () -> assertEquals(List.of("office", "wireless"), dto.tags()),
                () -> assertEquals(updatedAt, dto.lastUpdatedAt()));
    }

    @Test
    void shouldMapDtoToAvroAndBack() {
        Instant updatedAt = Instant.parse("2026-08-26T09:04:00Z");

        MarketingDto source = MarketingDto.builder()
                .productId("P001")
                .shortDescription("Wireless Keyboard")
                .longDescription("Ergonomic wireless keyboard")
                .tags(List.of("office", "wireless"))
                .lastUpdatedAt(updatedAt)
                .build();

        MarketingDto result = mapper.fromAvro(mapper.toAvro(source));

        assertEquals(source, result);
    }

    @Test
    void shouldReturnNullWhenDtoIsNull() {
        Marketing event = mapper.toAvro(null);
        assertNull(event);
    }

    @Test
    void shouldReturnNullWhenEventIsNull() {
        MarketingDto dto = mapper.fromAvro(null);
        assertNull(dto);
    }
}
