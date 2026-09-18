package com.qbe.kafkastarter.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.qbe.avro.Price;
import com.qbe.kafkastarter.constants.TopicsConstants;
import com.qbe.kafkastarter.dto.PriceDto;
import com.qbe.kafkastarter.mapper.PriceMapper;
import java.math.BigDecimal;
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
class TestPriceController {

    @Mock
    private KafkaTemplate<String, Price> kafkaTemplate;

    @Mock
    private PriceMapper priceMapper;

    @InjectMocks
    private PriceController priceController;

    private PriceDto priceDto;
    private Price price;

    @BeforeEach
    void setUp() {
        priceDto = PriceDto.builder()
                .productId("P001")
                .amount(BigDecimal.valueOf(49.99))
                .currency("EUR")
                .lastUpdatedAt(Instant.parse("2026-08-26T09:01:00Z"))
                .build();

        price = Price.newBuilder()
                .setProductId("P001")
                .setAmount(49.99)
                .setCurrency("EUR")
                .setLastUpdatedAt(Instant.parse("2026-08-26T09:01:00Z"))
                .build();
    }

    @Test
    void shouldPublishPriceEventAndReturnCreatedResponse() {
        when(priceMapper.toAvro(priceDto)).thenReturn(price);

        ResponseEntity<PriceDto> response = priceController.createPrice(priceDto);

        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        assertEquals(priceDto, response.getBody());

        verify(priceMapper).toAvro(priceDto);
        verify(kafkaTemplate).send(TopicsConstants.TOPIC_PRICE, priceDto.productId(), price);
    }
}
