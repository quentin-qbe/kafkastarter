package com.qbe.kafkastarter.mapper;

import static org.assertj.core.api.Assertions.assertThat;

import com.qbe.avro.StockAlertState;
import com.qbe.kafkastarter.dto.StockAlertStateDto;
import org.junit.jupiter.api.Test;
import org.mapstruct.factory.Mappers;

class TestStockAlertStateMapper {

    private final StockAlertStateMapper mapper = Mappers.getMapper(StockAlertStateMapper.class);

    @Test
    void shouldMapDtoToAvro() {
        StockAlertStateDto dto = StockAlertStateDto.builder()
                .alertActive(true)
                .severity("CRITICAL")
                .lastKnownStock(3)
                .lastAlertDate("2026-08-31T14:25:30Z")
                .build();

        StockAlertState result = mapper.toAvro(dto);

        assertThat(result).isNotNull();
        assertThat(result.getAlertActive()).isTrue();
        assertThat(result.getSeverity()).isEqualTo("CRITICAL");
        assertThat(result.getLastKnownStock()).isEqualTo(3);
        assertThat(result.getLastAlertDate()).isEqualTo("2026-08-31T14:25:30Z");
    }

    @Test
    void shouldMapAvroToDto() {
        StockAlertState event = StockAlertState.newBuilder()
                .setAlertActive(true)
                .setSeverity("CRITICAL")
                .setLastKnownStock(3)
                .setLastAlertDate("2026-08-31T14:25:30Z")
                .build();

        StockAlertStateDto result = mapper.fromAvro(event);

        assertThat(result).isNotNull();
        assertThat(result.alertActive()).isTrue();
        assertThat(result.severity()).isEqualTo("CRITICAL");
        assertThat(result.lastKnownStock()).isEqualTo(3);
        assertThat(result.lastAlertDate()).isEqualTo("2026-08-31T14:25:30Z");
    }

    @Test
    void shouldMapNullDtoToNullAvro() {
        assertThat(mapper.toAvro(null)).isNull();
    }

    @Test
    void shouldMapNullAvroToNullDto() {
        assertThat(mapper.fromAvro(null)).isNull();
    }

    @Test
    void shouldPerformRoundTripMapping() {
        StockAlertStateDto source = StockAlertStateDto.builder()
                .alertActive(true)
                .severity("WARNING")
                .lastKnownStock(10)
                .lastAlertDate("2026-08-31T14:25:30Z")
                .build();

        StockAlertStateDto result = mapper.fromAvro(mapper.toAvro(source));

        assertThat(result).isNotNull();
        assertThat(result.alertActive()).isEqualTo(source.alertActive());
        assertThat(result.severity()).isEqualTo(source.severity());
        assertThat(result.lastKnownStock()).isEqualTo(source.lastKnownStock());
        assertThat(result.lastAlertDate()).isEqualTo(source.lastAlertDate());
    }
}
