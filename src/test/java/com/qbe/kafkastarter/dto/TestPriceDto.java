package com.qbe.kafkastarter.dto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestPriceDto {

    private Validator validator;

    @BeforeEach
    void setUp() {
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        validator = factory.getValidator();
    }

    @Test
    void shouldValidatePriceDto() {

        PriceDto dto = PriceDto.builder()
                .productId("P001")
                .amount(BigDecimal.valueOf(49.99))
                .currency("EUR")
                .lastUpdatedAt(Instant.now())
                .build();

        Set<ConstraintViolation<PriceDto>> violations = validator.validate(dto);

        assertTrue(violations.isEmpty());
    }

    @Test
    void shouldRejectBlankProductId() {

        PriceDto dto = PriceDto.builder()
                .productId("")
                .amount(BigDecimal.valueOf(49.99))
                .currency("EUR")
                .lastUpdatedAt(Instant.now())
                .build();

        Set<ConstraintViolation<PriceDto>> violations = validator.validate(dto);

        assertEquals(2, violations.size());
    }

    @Test
    void shouldRejectInvalidProductIdFormat() {

        PriceDto dto = PriceDto.builder()
                .productId("ABC")
                .amount(BigDecimal.valueOf(49.99))
                .currency("EUR")
                .lastUpdatedAt(Instant.now())
                .build();

        Set<ConstraintViolation<PriceDto>> violations = validator.validate(dto);

        assertEquals(1, violations.size());
    }

    @Test
    void shouldRejectNullAmount() {

        PriceDto dto = PriceDto.builder()
                .productId("P001")
                .amount(null)
                .currency("EUR")
                .lastUpdatedAt(Instant.now())
                .build();

        Set<ConstraintViolation<PriceDto>> violations = validator.validate(dto);

        assertEquals(1, violations.size());
    }

    @Test
    void shouldRejectZeroAmount() {

        PriceDto dto = PriceDto.builder()
                .productId("P001")
                .amount(BigDecimal.ZERO)
                .currency("EUR")
                .lastUpdatedAt(Instant.now())
                .build();

        Set<ConstraintViolation<PriceDto>> violations = validator.validate(dto);

        assertEquals(1, violations.size());
    }

    @Test
    void shouldRejectNegativeAmount() {

        PriceDto dto = PriceDto.builder()
                .productId("P001")
                .amount(BigDecimal.valueOf(-10))
                .currency("EUR")
                .lastUpdatedAt(Instant.now())
                .build();

        Set<ConstraintViolation<PriceDto>> violations = validator.validate(dto);

        assertEquals(1, violations.size());
    }

    @Test
    void shouldRejectBlankCurrency() {

        PriceDto dto = PriceDto.builder()
                .productId("P001")
                .amount(BigDecimal.valueOf(49.99))
                .currency("")
                .lastUpdatedAt(Instant.parse("2026-08-26T09:04:00Z"))
                .build();

        Set<ConstraintViolation<PriceDto>> violations = validator.validate(dto);

        assertEquals(2, violations.size());
    }

    @Test
    void shouldRejectInvalidCurrencyFormat() {

        PriceDto dto = PriceDto.builder()
                .productId("P001")
                .amount(BigDecimal.valueOf(49.99))
                .currency("Euro")
                .lastUpdatedAt(Instant.parse("2026-08-26T09:04:00Z"))
                .build();

        Set<ConstraintViolation<PriceDto>> violations = validator.validate(dto);

        assertEquals(1, violations.size());
    }

    @Test
    void shouldRejectFutureLastUpdatedAt() {

        PriceDto dto = PriceDto.builder()
                .productId("P001")
                .amount(BigDecimal.valueOf(49.99))
                .currency("EUR")
                .lastUpdatedAt(Instant.now().plus(1, ChronoUnit.DAYS))
                .build();

        Set<ConstraintViolation<PriceDto>> violations = validator.validate(dto);

        assertEquals(1, violations.size());
    }

    @Test
    void shouldReturnExpectedMessageForInvalidCurrency() {

        PriceDto dto = PriceDto.builder()
                .productId("P001")
                .amount(BigDecimal.valueOf(49.99))
                .currency("eur")
                .lastUpdatedAt(Instant.now())
                .build();

        Set<ConstraintViolation<PriceDto>> violations = validator.validate(dto);

        ConstraintViolation<PriceDto> violation = violations.iterator().next();

        assertEquals("currency must be a valid ISO code", violation.getMessage());
    }
}
