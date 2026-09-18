package com.qbe.kafkastarter.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.qbe.avro.Stock;
import com.qbe.kafkastarter.constants.TopicsConstants;
import com.qbe.kafkastarter.dto.StockDto;
import com.qbe.kafkastarter.mapper.StockMapper;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;

@ExtendWith(MockitoExtension.class)
class TestStockController {

    @Mock
    private KafkaTemplate<String, Stock> kafkaTemplate;

    @Mock
    private StockMapper stockMapper;

    @InjectMocks
    private StockController stockController;

    private StockDto stockDto;
    private Stock stock;

    @BeforeEach
    void setUp() {
        stockDto = StockDto.builder()
                .productId("P001")
                .availableQuantity(120)
                .reservedQuantity(10)
                .warehouseCode("WH001")
                .lastUpdatedAt(Instant.parse("2026-08-26T09:02:00Z"))
                .build();

        stock = Stock.newBuilder()
                .setProductId("P001")
                .setAvailableQuantity(120)
                .setReservedQuantity(10)
                .setWarehouseCode("WH001")
                .setLastUpdatedAt(Instant.parse("2026-08-26T09:02:00Z"))
                .build();
    }

    @Test
    void shouldPublishStockEventAndReturnCreatedResponse() {
        when(stockMapper.toAvro(stockDto)).thenReturn(stock);

        ResponseEntity<StockDto> response = stockController.createStock(stockDto);

        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        assertEquals(stockDto, response.getBody());

        verify(stockMapper).toAvro(stockDto);
        verify(kafkaTemplate).send(TopicsConstants.TOPIC_STOCK, stockDto.productId(), stock);
    }
}
