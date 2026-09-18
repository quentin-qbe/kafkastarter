package com.qbe.kafkastarter.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.qbe.avro.Product;
import com.qbe.kafkastarter.constants.TopicsConstants;
import com.qbe.kafkastarter.dto.ProductDto;
import com.qbe.kafkastarter.mapper.ProductMapper;
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
class TestProductController {

    @Mock
    private KafkaTemplate<String, Product> kafkaTemplate;

    @Mock
    private ProductMapper productMapper;

    @InjectMocks
    private ProductController productController;

    private ProductDto productDto;
    private Product product;

    @BeforeEach
    void setUp() {

        productDto = ProductDto.builder()
                .productId("P001")
                .sku("KB001")
                .name("Keyboard")
                .brand("Microsoft")
                .category("Accessories")
                .active(true)
                .createdAt(Instant.parse("2026-08-26T09:00:00Z"))
                .build();

        product = Product.newBuilder()
                .setProductId("P001")
                .setSku("KB001")
                .setName("Keyboard")
                .setBrand("Microsoft")
                .setCategory("Accessories")
                .setActive(true)
                .setCreatedAt(Instant.parse("2026-08-26T09:00:00Z"))
                .build();
    }

    @Test
    void shouldPublishProductEventAndReturnCreatedResponse() {
        when(productMapper.toAvro(productDto)).thenReturn(product);

        ResponseEntity<ProductDto> response = productController.createProduct(productDto);

        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        assertEquals(productDto, response.getBody());

        verify(productMapper).toAvro(productDto);
        verify(kafkaTemplate).send(TopicsConstants.TOPIC_PRODUCT, productDto.productId(), product);
    }
}
