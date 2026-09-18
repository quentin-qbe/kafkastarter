package com.qbe.kafkastarter.exporter;

import static org.junit.jupiter.api.Assertions.*;

import com.qbe.kafkastarter.dto.ProductMasterDto;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Comparator;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

@Slf4j
class TestCsvExporter {

    private final CsvExporter csvExporter = new CsvExporter();

    @AfterEach
    void cleanup() throws Exception {
        Path exports = Path.of("exports");

        if (Files.exists(exports)) {
            Files.walk(exports).sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.delete(path);
                } catch (Exception ignored) {
                    log.error(ignored.getMessage(), ignored);
                }
            });
        }
    }

    @Test
    void shouldCreateCsvFile() throws Exception {
        ProductMasterDto dto = ProductMasterDto.builder()
                .productId("P001")
                .sku("KB001")
                .name("Keyboard")
                .price(BigDecimal.valueOf(49.99))
                .currency("EUR")
                .availableStock(120)
                .lastUpdatedAt(Instant.parse("2026-08-26T09:04:00Z"))
                .build();

        csvExporter.export(dto);

        Path file = Path.of("exports", "P001.csv");

        assertTrue(Files.exists(file));
    }

    @Test
    void shouldWriteExpectedContent() throws Exception {
        ProductMasterDto dto = ProductMasterDto.builder()
                .productId("P001")
                .sku("KB001")
                .name("Keyboard")
                .price(BigDecimal.valueOf(49.99))
                .currency("EUR")
                .availableStock(120)
                .lastUpdatedAt(Instant.parse("2026-08-26T09:04:00Z"))
                .build();

        csvExporter.export(dto);

        Path file = Path.of("exports", "P001.csv");

        String content = Files.readString(file);

        String expected =
                """
                productId;sku;name;price;currency;availableStock;updatedAt
                P001;KB001;Keyboard;49.99;EUR;120;2026-08-26T09:04:00Z
                """;

        assertEquals(expected.replace("\r\n", "\n"), content.replace("\r\n", "\n"));
    }

    @Test
    void shouldOverwriteExistingFile() throws Exception {
        ProductMasterDto first = ProductMasterDto.builder()
                .productId("P001")
                .sku("KB001")
                .name("Keyboard")
                .price(BigDecimal.valueOf(49.99))
                .currency("EUR")
                .availableStock(120)
                .lastUpdatedAt(Instant.parse("2026-08-26T09:04:00Z"))
                .build();

        ProductMasterDto second = ProductMasterDto.builder()
                .productId("P001")
                .sku("KB001")
                .name("Mechanical Keyboard")
                .price(BigDecimal.valueOf(99.99))
                .currency("EUR")
                .availableStock(50)
                .lastUpdatedAt(Instant.parse("2026-08-27T09:04:00Z"))
                .build();

        csvExporter.export(first);
        csvExporter.export(second);

        Path file = Path.of("exports", "P001.csv");

        String content = Files.readString(file);

        assertTrue(content.contains("Mechanical Keyboard"));
        assertTrue(content.contains("99.99"));
        assertFalse(content.contains("49.99"));
    }

    @Test
    void shouldCreateExportsDirectoryIfMissing() throws Exception {
        Path exportsDir = Path.of("exports");

        if (Files.exists(exportsDir)) {
            Files.walk(exportsDir).sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.delete(path);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            });
        }

        ProductMasterDto dto = ProductMasterDto.builder()
                .productId("P001")
                .sku("KB001")
                .name("Keyboard")
                .price(BigDecimal.valueOf(49.99))
                .currency("EUR")
                .availableStock(120)
                .lastUpdatedAt(Instant.parse("2026-08-26T09:04:00Z"))
                .build();

        csvExporter.export(dto);

        assertTrue(Files.exists(exportsDir));
        assertTrue(Files.isDirectory(exportsDir));

        Path csvFile = exportsDir.resolve("P001.csv");

        assertTrue(Files.exists(csvFile));
    }
}
