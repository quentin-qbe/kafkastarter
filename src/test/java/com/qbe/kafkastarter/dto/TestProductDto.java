package com.qbe.kafkastarter.dto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestProductDto {

    private Validator validator;

    @BeforeEach
    void setUp() {
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        validator = factory.getValidator();
    }

    @Test
    void shouldValidateProductDto() {
        ProductDto dto = ProductDto.builder()
                .productId("P001")
                .sku("KB001")
                .name("Keyboard")
                .category("Accessories")
                .brand("Microsoft")
                .active(true)
                .alertThreshold(40)
                .createdAt(Instant.now())
                .build();

        Set<ConstraintViolation<ProductDto>> violations = validator.validate(dto);

        assertTrue(violations.isEmpty());
    }

    @Test
    void shouldRejectBlankProductId() {
        ProductDto dto = ProductDto.builder()
                .productId("")
                .sku("KB001")
                .name("Keyboard")
                .category("Accessories")
                .brand("Microsoft")
                .active(true)
                .alertThreshold(40)
                .createdAt(Instant.parse("2026-08-26T09:04:00Z"))
                .build();

        Set<ConstraintViolation<ProductDto>> violations = validator.validate(dto);

        assertEquals(2, violations.size());
    }

    @Test
    void shouldRejectInvalidProductIdFormat() {
        ProductDto dto = ProductDto.builder()
                .productId("ABC")
                .sku("KB001")
                .name("Keyboard")
                .category("Accessories")
                .brand("Microsoft")
                .active(true)
                .alertThreshold(40)
                .createdAt(Instant.parse("2026-08-26T09:04:00Z"))
                .build();

        Set<ConstraintViolation<ProductDto>> violations = validator.validate(dto);

        assertEquals(1, violations.size());
    }

    @Test
    void shouldRejectBlankSku() {
        ProductDto dto = ProductDto.builder()
                .productId("P001")
                .sku("")
                .name("Keyboard")
                .category("Accessories")
                .brand("Microsoft")
                .active(true)
                .alertThreshold(40)
                .createdAt(Instant.now())
                .build();

        Set<ConstraintViolation<ProductDto>> violations = validator.validate(dto);

        assertEquals(2, violations.size());
    }

    @Test
    void shouldRejectInvalidSkuFormat() {
        ProductDto dto = ProductDto.builder()
                .productId("P001")
                .sku("KB 001")
                .name("Keyboard")
                .category("Accessories")
                .brand("Microsoft")
                .active(true)
                .alertThreshold(40)
                .createdAt(Instant.now())
                .build();

        Set<ConstraintViolation<ProductDto>> violations = validator.validate(dto);

        assertEquals(1, violations.size());
    }

    @Test
    void shouldRejectSkuLongerThan50Characters() {
        ProductDto dto = ProductDto.builder()
                .productId("P001")
                .sku("A".repeat(51))
                .name("Keyboard")
                .category("Accessories")
                .brand("Microsoft")
                .active(true)
                .alertThreshold(40)
                .createdAt(Instant.now())
                .build();

        Set<ConstraintViolation<ProductDto>> violations = validator.validate(dto);

        assertEquals(1, violations.size());
    }

    @Test
    void shouldRejectBlankName() {
        ProductDto dto = ProductDto.builder()
                .productId("P001")
                .sku("KB001")
                .name("")
                .category("Accessories")
                .brand("Microsoft")
                .active(true)
                .alertThreshold(40)
                .createdAt(Instant.now())
                .build();

        Set<ConstraintViolation<ProductDto>> violations = validator.validate(dto);

        assertEquals(1, violations.size());
    }

    @Test
    void shouldRejectBlankCategory() {
        ProductDto dto = ProductDto.builder()
                .productId("P001")
                .sku("KB001")
                .name("Keyboard")
                .category("")
                .brand("Microsoft")
                .active(true)
                .alertThreshold(40)
                .createdAt(Instant.now())
                .build();

        Set<ConstraintViolation<ProductDto>> violations = validator.validate(dto);

        assertEquals(1, violations.size());
    }

    @Test
    void shouldRejectBlankBrand() {
        ProductDto dto = ProductDto.builder()
                .productId("P001")
                .sku("KB001")
                .name("Keyboard")
                .category("Accessories")
                .brand("")
                .active(true)
                .alertThreshold(40)
                .createdAt(Instant.now())
                .build();

        Set<ConstraintViolation<ProductDto>> violations = validator.validate(dto);

        assertEquals(1, violations.size());
    }

    @Test
    void shouldRejectFutureCreatedAt() {
        ProductDto dto = ProductDto.builder()
                .productId("P001")
                .sku("KB001")
                .name("Keyboard")
                .category("Accessories")
                .brand("Microsoft")
                .active(true)
                .alertThreshold(40)
                .createdAt(Instant.now().plus(1, ChronoUnit.DAYS))
                .build();

        Set<ConstraintViolation<ProductDto>> violations = validator.validate(dto);

        assertEquals(1, violations.size());
    }

    @Test
    void shouldReturnExpectedMessageForInvalidSku() {
        ProductDto dto = ProductDto.builder()
                .productId("P001")
                .sku("KB 001")
                .name("Keyboard")
                .category("Accessories")
                .brand("Microsoft")
                .active(true)
                .alertThreshold(40)
                .createdAt(Instant.now())
                .build();

        Set<ConstraintViolation<ProductDto>> violations = validator.validate(dto);
        ConstraintViolation<ProductDto> violation = violations.iterator().next();
        assertEquals("sku contains invalid characters", violation.getMessage());
    }

    @Test
    void shouldRejectNullAlertThreshold() {
        ProductDto dto = ProductDto.builder()
                .productId("P001")
                .sku("KB001")
                .name("Keyboard")
                .category("Accessories")
                .brand("Microsoft")
                .active(true)
                .createdAt(Instant.now())
                .build();

        Set<ConstraintViolation<ProductDto>> violations = validator.validate(dto);
        ConstraintViolation<ProductDto> violation = violations.iterator().next();
        assertEquals("alertThreshold is mandatory", violation.getMessage());
    }

    @Test
    void shouldRejectOnAlertThresholdOver200() {
        ProductDto dto = ProductDto.builder()
                .productId("P001")
                .sku("KB001")
                .name("Keyboard")
                .category("Accessories")
                .brand("Microsoft")
                .active(true)
                .alertThreshold(300)
                .createdAt(Instant.now())
                .build();

        Set<ConstraintViolation<ProductDto>> violations = validator.validate(dto);
        ConstraintViolation<ProductDto> violation = violations.iterator().next();
        assertEquals("alertThreshold must be less than or equal to 200", violation.getMessage());
    }
}
