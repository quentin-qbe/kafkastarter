package com.qbe.kafkastarter.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import com.qbe.avro.StockAlertState;
import com.qbe.kafkastarter.dto.StockAlertStateDto;
import com.qbe.kafkastarter.mapper.StockAlertStateMapper;
import com.qbe.kafkastarter.service.StockAlertService;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

@ExtendWith(MockitoExtension.class)
class TestStockAlertController {

    @Mock
    private StockAlertService stockAlertService;

    @Mock
    private StockAlertStateMapper stockAlertStateMapper;

    @InjectMocks
    private StockAlertController stockAlertController;

    private StockAlertState stockAlertState;
    private StockAlertStateDto stockAlertStateDto;

    @BeforeEach
    void setUp() {
        stockAlertState = StockAlertState.newBuilder()
                .setProductId("P001")
                .setAlertActive(true)
                .setSeverity("CRITICAL")
                .setLastKnownStock(3)
                .setLastAlertDate("2026-08-31T14:25:30Z")
                .build();

        stockAlertStateDto = StockAlertStateDto.builder()
                .alertActive(true)
                .severity("CRITICAL")
                .lastKnownStock(3)
                .lastAlertDate("2026-08-31T14:25:30Z")
                .build();
    }

    @Test
    void shouldReturnAllAlerts() {
        when(stockAlertService.findAll()).thenReturn(List.of(stockAlertState));
        when(stockAlertStateMapper.fromAvro(stockAlertState)).thenReturn(stockAlertStateDto);

        List<StockAlertStateDto> result = stockAlertController.getAllAlerts();
        assertThat(result).hasSize(1).containsExactly(stockAlertStateDto);

        verify(stockAlertService).findAll();
        verify(stockAlertStateMapper).fromAvro(stockAlertState);
    }

    @Test
    void shouldReturnAlertWhenProductExists() {
        when(stockAlertService.findByProductId("P001")).thenReturn(stockAlertState);

        when(stockAlertStateMapper.fromAvro(stockAlertState)).thenReturn(stockAlertStateDto);

        ResponseEntity<StockAlertStateDto> response = stockAlertController.getAlertByProductId("P001");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isEqualTo(stockAlertStateDto);

        verify(stockAlertService).findByProductId("P001");
        verify(stockAlertStateMapper).fromAvro(stockAlertState);
    }

    @Test
    void shouldReturnNotFoundWhenProductDoesNotExist() {
        when(stockAlertService.findByProductId("UNKNOWN")).thenReturn(null);

        ResponseEntity<StockAlertStateDto> response = stockAlertController.getAlertByProductId("UNKNOWN");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(response.getBody()).isNull();

        verify(stockAlertService).findByProductId("UNKNOWN");
        verifyNoInteractions(stockAlertStateMapper);
    }

    @Test
    void shouldReturnEmptyListWhenNoAlertsExist() {
        when(stockAlertService.findAll()).thenReturn(List.of());

        List<StockAlertStateDto> result = stockAlertController.getAllAlerts();
        assertThat(result).isEmpty();

        verify(stockAlertService).findAll();
        verifyNoInteractions(stockAlertStateMapper);
    }
}
