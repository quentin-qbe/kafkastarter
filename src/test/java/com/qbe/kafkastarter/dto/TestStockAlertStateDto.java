package com.qbe.kafkastarter.dto;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class TestStockAlertStateDto {

    @Test
    void shouldBuildDto() {
        StockAlertStateDto dto = StockAlertStateDto.builder()
                .alertActive(true)
                .severity("CRITICAL")
                .lastKnownStock(3)
                .lastAlertDate("2026-08-31T14:25:30Z")
                .build();

        assertThat(dto.alertActive()).isTrue();
        assertThat(dto.severity()).isEqualTo("CRITICAL");
        assertThat(dto.lastKnownStock()).isEqualTo(3);
        assertThat(dto.lastAlertDate()).isEqualTo("2026-08-31T14:25:30Z");
    }

    @Test
    void shouldImplementEqualsAndHashCode() {
        StockAlertStateDto dto1 = StockAlertStateDto.builder()
                .alertActive(true)
                .severity("CRITICAL")
                .lastKnownStock(3)
                .lastAlertDate("2026-08-31T14:25:30Z")
                .build();

        StockAlertStateDto dto2 = StockAlertStateDto.builder()
                .alertActive(true)
                .severity("CRITICAL")
                .lastKnownStock(3)
                .lastAlertDate("2026-08-31T14:25:30Z")
                .build();

        assertThat(dto1).isEqualTo(dto2).hasSameHashCodeAs(dto2);
    }

    @Test
    void shouldImplementToString() {
        StockAlertStateDto dto = StockAlertStateDto.builder()
                .alertActive(true)
                .severity("CRITICAL")
                .lastKnownStock(3)
                .lastAlertDate("2026-08-31T14:25:30Z")
                .build();

        assertThat(dto.toString())
                .contains("alertActive=true")
                .contains("severity=CRITICAL")
                .contains("lastKnownStock=3")
                .contains("lastAlertDate=2026-08-31T14:25:30Z");
    }

    @Test
    void shouldSupportNullValues() {
        StockAlertStateDto dto = StockAlertStateDto.builder()
                .alertActive(false)
                .severity(null)
                .lastKnownStock(null)
                .lastAlertDate(null)
                .build();

        assertThat(dto.alertActive()).isFalse();
        assertThat(dto.severity()).isNull();
        assertThat(dto.lastKnownStock()).isNull();
        assertThat(dto.lastAlertDate()).isNull();
    }
}
