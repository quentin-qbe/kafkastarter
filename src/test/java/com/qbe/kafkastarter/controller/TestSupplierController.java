package com.qbe.kafkastarter.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.qbe.avro.Supplier;
import com.qbe.kafkastarter.constants.TopicsConstants;
import com.qbe.kafkastarter.dto.SupplierDto;
import com.qbe.kafkastarter.mapper.SupplierMapper;
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
class TestSupplierController {

    @Mock
    private KafkaTemplate<String, Supplier> kafkaTemplate;

    @Mock
    private SupplierMapper supplierMapper;

    @InjectMocks
    private SupplierController supplierController;

    private SupplierDto supplierDto;
    private Supplier supplier;

    @BeforeEach
    void setUp() {
        supplierDto = SupplierDto.builder()
                .productId("P001")
                .supplierId("SUP001")
                .supplierName("TechSupplier")
                .supplierCountry("France")
                .lastUpdatedAt(Instant.parse("2026-08-26T09:03:00Z"))
                .build();

        supplier = Supplier.newBuilder()
                .setProductId("P001")
                .setSupplierId("SUP001")
                .setSupplierName("TechSupplier")
                .setSupplierCountry("France")
                .setLastUpdatedAt(Instant.parse("2026-08-26T09:03:00Z"))
                .build();
    }

    @Test
    void shouldPublishSupplierEventAndReturnCreatedResponse() {
        when(supplierMapper.toAvro(supplierDto)).thenReturn(supplier);

        ResponseEntity<SupplierDto> response = supplierController.createSupplier(supplierDto);

        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        assertEquals(supplierDto, response.getBody());

        verify(supplierMapper).toAvro(supplierDto);
        verify(kafkaTemplate).send(TopicsConstants.TOPIC_SUPPLIER, supplierDto.productId(), supplier);
    }
}
