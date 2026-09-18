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

class TestSupplierDto {

    private Validator validator;

    @BeforeEach
    void setUp() {
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        validator = factory.getValidator();
    }

    @Test
    void shouldValidateSupplierDto() {
        SupplierDto dto = SupplierDto.builder()
                .productId("P001")
                .supplierId("SUP001")
                .supplierName("TechSupplier")
                .supplierCountry("France")
                .lastUpdatedAt(Instant.parse("2026-08-26T09:04:00Z"))
                .build();

        Set<ConstraintViolation<SupplierDto>> violations = validator.validate(dto);

        assertTrue(violations.isEmpty());
    }

    @Test
    void shouldRejectBlankProductId() {
        SupplierDto dto = SupplierDto.builder()
                .productId("")
                .supplierId("SUP001")
                .supplierName("TechSupplier")
                .supplierCountry("France")
                .lastUpdatedAt(Instant.now())
                .build();

        Set<ConstraintViolation<SupplierDto>> violations = validator.validate(dto);

        assertEquals(2, violations.size());
    }

    @Test
    void shouldRejectInvalidProductIdFormat() {
        SupplierDto dto = SupplierDto.builder()
                .productId("P1")
                .supplierId("SUP001")
                .supplierName("TechSupplier")
                .supplierCountry("France")
                .lastUpdatedAt(Instant.parse("2026-08-26T09:04:00Z"))
                .build();

        Set<ConstraintViolation<SupplierDto>> violations = validator.validate(dto);

        assertEquals(1, violations.size());
    }

    @Test
    void shouldRejectBlankSupplierId() {
        SupplierDto dto = SupplierDto.builder()
                .productId("P001")
                .supplierId("")
                .supplierName("TechSupplier")
                .supplierCountry("France")
                .lastUpdatedAt(Instant.now())
                .build();

        Set<ConstraintViolation<SupplierDto>> violations = validator.validate(dto);

        assertEquals(2, violations.size());
    }

    @Test
    void shouldRejectInvalidSupplierIdFormat() {
        SupplierDto dto = SupplierDto.builder()
                .productId("P001")
                .supplierId("SUP1")
                .supplierName("TechSupplier")
                .supplierCountry("France")
                .lastUpdatedAt(Instant.parse("2026-08-26T09:04:00Z"))
                .build();

        Set<ConstraintViolation<SupplierDto>> violations = validator.validate(dto);

        assertEquals(1, violations.size());
    }

    @Test
    void shouldRejectBlankSupplierName() {
        SupplierDto dto = SupplierDto.builder()
                .productId("P001")
                .supplierId("SUP001")
                .supplierName("")
                .supplierCountry("France")
                .lastUpdatedAt(Instant.parse("2026-08-26T09:04:00Z"))
                .build();

        Set<ConstraintViolation<SupplierDto>> violations = validator.validate(dto);

        assertEquals(1, violations.size());
    }

    @Test
    void shouldRejectSupplierNameLongerThan255Characters() {
        SupplierDto dto = SupplierDto.builder()
                .productId("P001")
                .supplierId("SUP001")
                .supplierName("A".repeat(256))
                .supplierCountry("France")
                .lastUpdatedAt(Instant.parse("2026-08-26T09:04:00Z"))
                .build();

        Set<ConstraintViolation<SupplierDto>> violations = validator.validate(dto);

        assertEquals(1, violations.size());
    }

    @Test
    void shouldRejectBlankSupplierCountry() {
        SupplierDto dto = SupplierDto.builder()
                .productId("P001")
                .supplierId("SUP001")
                .supplierName("TechSupplier")
                .supplierCountry("")
                .lastUpdatedAt(Instant.parse("2026-08-26T09:04:00Z"))
                .build();

        Set<ConstraintViolation<SupplierDto>> violations = validator.validate(dto);

        assertEquals(1, violations.size());
    }

    @Test
    void shouldRejectSupplierCountryLongerThan100Characters() {
        SupplierDto dto = SupplierDto.builder()
                .productId("P001")
                .supplierId("SUP001")
                .supplierName("TechSupplier")
                .supplierCountry("A".repeat(101))
                .lastUpdatedAt(Instant.parse("2026-08-26T09:04:00Z"))
                .build();

        Set<ConstraintViolation<SupplierDto>> violations = validator.validate(dto);

        assertEquals(1, violations.size());
    }

    @Test
    void shouldRejectNullLastUpdatedAt() {
        SupplierDto dto = SupplierDto.builder()
                .productId("P001")
                .supplierId("SUP001")
                .supplierName("TechSupplier")
                .supplierCountry("France")
                .lastUpdatedAt(null)
                .build();

        Set<ConstraintViolation<SupplierDto>> violations = validator.validate(dto);

        assertEquals(1, violations.size());
    }

    @Test
    void shouldRejectFutureLastUpdatedAt() {
        SupplierDto dto = SupplierDto.builder()
                .productId("P001")
                .supplierId("SUP001")
                .supplierName("TechSupplier")
                .supplierCountry("France")
                .lastUpdatedAt(Instant.now().plus(1, ChronoUnit.DAYS))
                .build();

        Set<ConstraintViolation<SupplierDto>> violations = validator.validate(dto);

        assertEquals(1, violations.size());
    }

    @Test
    void shouldReturnExpectedMessageForInvalidSupplierId() {
        SupplierDto dto = SupplierDto.builder()
                .productId("P001")
                .supplierId("ABC")
                .supplierName("TechSupplier")
                .supplierCountry("France")
                .lastUpdatedAt(Instant.parse("2026-08-26T09:04:00Z"))
                .build();

        Set<ConstraintViolation<SupplierDto>> violations = validator.validate(dto);

        ConstraintViolation<SupplierDto> violation = violations.iterator().next();

        assertEquals("supplierId must follow format SUP001", violation.getMessage());
    }
}
