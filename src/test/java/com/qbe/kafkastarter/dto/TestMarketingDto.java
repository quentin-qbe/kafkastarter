package com.qbe.kafkastarter.dto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestMarketingDto {

    private Validator validator;

    @BeforeEach
    void setUp() {
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        validator = factory.getValidator();
    }

    @Test
    void shouldValidateMarketingDto() {

        MarketingDto dto = MarketingDto.builder()
                .productId("P001")
                .shortDescription("Wireless Keyboard")
                .longDescription("Ergonomic wireless keyboard")
                .tags(List.of("office", "wireless"))
                .lastUpdatedAt(Instant.now())
                .build();

        Set<ConstraintViolation<MarketingDto>> violations = validator.validate(dto);

        assertTrue(violations.isEmpty());
    }

    @Test
    void shouldRejectBlankProductId() {

        MarketingDto dto = MarketingDto.builder()
                .productId("")
                .shortDescription("Wireless Keyboard")
                .longDescription("Ergonomic wireless keyboard")
                .tags(List.of("office"))
                .lastUpdatedAt(Instant.now())
                .build();

        Set<ConstraintViolation<MarketingDto>> violations = validator.validate(dto);

        assertEquals(1, violations.size());
    }

    @Test
    void shouldRejectEmptyTags() {

        MarketingDto dto = MarketingDto.builder()
                .productId("P001")
                .shortDescription("Wireless Keyboard")
                .longDescription("Ergonomic wireless keyboard")
                .tags(List.of())
                .lastUpdatedAt(Instant.now())
                .build();

        Set<ConstraintViolation<MarketingDto>> violations = validator.validate(dto);

        assertEquals(1, violations.size());
    }

    @Test
    void shouldRejectBlankTag() {

        MarketingDto dto = MarketingDto.builder()
                .productId("P001")
                .shortDescription("Wireless Keyboard")
                .longDescription("Ergonomic wireless keyboard")
                .tags(List.of(""))
                .lastUpdatedAt(Instant.now())
                .build();

        Set<ConstraintViolation<MarketingDto>> violations = validator.validate(dto);

        assertEquals(1, violations.size());
    }

    @Test
    void shouldRejectFutureLastUpdatedAt() {

        MarketingDto dto = MarketingDto.builder()
                .productId("P001")
                .shortDescription("Wireless Keyboard")
                .longDescription("Ergonomic wireless keyboard")
                .tags(List.of("office"))
                .lastUpdatedAt(Instant.now().plus(1, ChronoUnit.DAYS))
                .build();

        Set<ConstraintViolation<MarketingDto>> violations = validator.validate(dto);

        assertEquals(1, violations.size());
    }
}
