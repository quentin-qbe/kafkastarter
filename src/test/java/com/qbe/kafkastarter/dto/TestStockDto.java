package com.qbe.kafkastarter.dto;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import java.time.Instant;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestStockDto {

    private Validator validator;

    @BeforeEach
    void setUp() {
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        validator = factory.getValidator();
    }

    @Test
    void shouldBuildValidStockDto() {
        Instant now = Instant.now();

        StockDto dto = StockDto.builder()
                .productId("P001")
                .availableQuantity(100)
                .reservedQuantity(10)
                .warehouseCode("WH-FR-01")
                .lastUpdatedAt(now)
                .build();

        assertThat(dto.productId()).isEqualTo("P001");
        assertThat(dto.availableQuantity()).isEqualTo(100);
        assertThat(dto.reservedQuantity()).isEqualTo(10);
        assertThat(dto.warehouseCode()).isEqualTo("WH-FR-01");
        assertThat(dto.lastUpdatedAt()).isEqualTo(now);
    }

    @Test
    void shouldHaveNoValidationErrorsForValidDto() {
        StockDto dto = StockDto.builder()
                .productId("P001")
                .availableQuantity(100)
                .reservedQuantity(10)
                .warehouseCode("WH-FR-01")
                .lastUpdatedAt(Instant.now())
                .build();

        Set<ConstraintViolation<StockDto>> violations = validator.validate(dto);
        assertThat(violations).isEmpty();
    }

    @Test
    void shouldFailWhenProductIdIsBlank() {
        StockDto dto = StockDto.builder()
                .productId("")
                .availableQuantity(100)
                .reservedQuantity(10)
                .warehouseCode("WH-FR-01")
                .lastUpdatedAt(Instant.now())
                .build();

        Set<ConstraintViolation<StockDto>> violations = validator.validate(dto);
        assertThat(violations).extracting(ConstraintViolation::getMessage).contains("productId is mandatory");
    }

    @Test
    void shouldFailWhenProductIdFormatIsInvalid() {
        StockDto dto = StockDto.builder()
                .productId("ABC001")
                .availableQuantity(100)
                .reservedQuantity(10)
                .warehouseCode("WH-FR-01")
                .lastUpdatedAt(Instant.now())
                .build();

        Set<ConstraintViolation<StockDto>> violations = validator.validate(dto);
        assertThat(violations)
                .extracting(ConstraintViolation::getMessage)
                .contains("productId must follow format P001");
    }

    @Test
    void shouldFailWhenAvailableQuantityIsNegative() {
        StockDto dto = StockDto.builder()
                .productId("P001")
                .availableQuantity(-1)
                .reservedQuantity(10)
                .warehouseCode("WH-FR-01")
                .lastUpdatedAt(Instant.now())
                .build();

        Set<ConstraintViolation<StockDto>> violations = validator.validate(dto);
        assertThat(violations)
                .extracting(ConstraintViolation::getMessage)
                .contains("availableQuantity must be greater than or equal to 0");
    }

    @Test
    void shouldFailWhenReservedQuantityIsNegative() {
        StockDto dto = StockDto.builder()
                .productId("P001")
                .availableQuantity(100)
                .reservedQuantity(-1)
                .warehouseCode("WH-FR-01")
                .lastUpdatedAt(Instant.now())
                .build();

        Set<ConstraintViolation<StockDto>> violations = validator.validate(dto);

        assertThat(violations)
                .extracting(ConstraintViolation::getMessage)
                .contains("reservedQuantity must be greater than or equal to 0");
    }

    @Test
    void shouldFailWhenWarehouseCodeIsBlank() {
        StockDto dto = StockDto.builder()
                .productId("P001")
                .availableQuantity(100)
                .reservedQuantity(10)
                .warehouseCode("")
                .lastUpdatedAt(Instant.now())
                .build();

        Set<ConstraintViolation<StockDto>> violations = validator.validate(dto);
        assertThat(violations).extracting(ConstraintViolation::getMessage).contains("warehouseCode is mandatory");
    }

    @Test
    void shouldFailWhenWarehouseCodeContainsInvalidCharacters() {
        StockDto dto = StockDto.builder()
                .productId("P001")
                .availableQuantity(100)
                .reservedQuantity(10)
                .warehouseCode("WH FR 01")
                .lastUpdatedAt(Instant.now())
                .build();

        Set<ConstraintViolation<StockDto>> violations = validator.validate(dto);
        assertThat(violations)
                .extracting(ConstraintViolation::getMessage)
                .contains("warehouseCode contains invalid characters");
    }

    @Test
    void shouldFailWhenWarehouseCodeExceedsMaxLength() {
        StockDto dto = StockDto.builder()
                .productId("P001")
                .availableQuantity(100)
                .reservedQuantity(10)
                .warehouseCode("A".repeat(51))
                .lastUpdatedAt(Instant.now())
                .build();

        Set<ConstraintViolation<StockDto>> violations = validator.validate(dto);
        assertThat(violations)
                .extracting(ConstraintViolation::getMessage)
                .contains("warehouseCode must not exceed 50 characters");
    }

    @Test
    void shouldFailWhenLastUpdatedAtIsInFuture() {
        StockDto dto = StockDto.builder()
                .productId("P001")
                .availableQuantity(100)
                .reservedQuantity(10)
                .warehouseCode("WH-FR-01")
                .lastUpdatedAt(Instant.now().plusSeconds(3600))
                .build();

        Set<ConstraintViolation<StockDto>> violations = validator.validate(dto);
        assertThat(violations)
                .extracting(ConstraintViolation::getMessage)
                .contains("lastUpdatedAt must be in the past or present");
    }
}
