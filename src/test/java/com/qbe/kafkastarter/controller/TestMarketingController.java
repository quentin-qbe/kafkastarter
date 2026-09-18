package com.qbe.kafkastarter.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.qbe.avro.Marketing;
import com.qbe.kafkastarter.constants.TopicsConstants;
import com.qbe.kafkastarter.dto.MarketingDto;
import com.qbe.kafkastarter.mapper.MarketingMapper;
import java.time.Instant;
import java.util.List;
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
class TestMarketingController {

    @Mock
    private KafkaTemplate<String, Marketing> kafkaTemplate;

    @Mock
    private MarketingMapper marketingMapper;

    @InjectMocks
    private MarketingController marketingController;

    private MarketingDto marketingDto;
    private Marketing marketing;

    @BeforeEach
    void setUp() {
        marketingDto = MarketingDto.builder()
                .productId("P001")
                .shortDescription("Wireless Keyboard")
                .longDescription("Ergonomic wireless keyboard")
                .tags(List.of("office", "wireless"))
                .lastUpdatedAt(Instant.parse("2026-08-26T09:04:00Z"))
                .build();

        marketing = Marketing.newBuilder()
                .setProductId("P001")
                .setShortDescription("Wireless Keyboard")
                .setLongDescription("Ergonomic wireless keyboard")
                .setTags(List.of("office", "wireless"))
                .setLastUpdatedAt(Instant.parse("2026-08-26T09:04:00Z"))
                .build();
    }

    @Test
    void shouldPublishMarketingEventAndReturnCreatedResponse() {
        when(marketingMapper.toAvro(marketingDto)).thenReturn(marketing);

        ResponseEntity<MarketingDto> response = marketingController.createMarketing(marketingDto);

        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        assertEquals(marketingDto, response.getBody());

        verify(marketingMapper).toAvro(marketingDto);
        verify(kafkaTemplate).send(TopicsConstants.TOPIC_MARKETING, marketingDto.productId(), marketing);
    }
}
