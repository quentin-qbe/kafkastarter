package com.qbe.kafkastarter.exporter;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.qbe.kafkastarter.dto.ProductMasterDto;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Comparator;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@Slf4j
class TestXmlExporter {

    private XmlExporter xmlExporter;

    @BeforeEach
    void setUp() {
        XmlMapper mapper = new XmlMapper();
        mapper.findAndRegisterModules();
        xmlExporter = new XmlExporter(mapper);
    }

    @AfterEach
    void cleanup() throws Exception {
        Path exports = Path.of("exports");

        if (Files.exists(exports)) {
            Files.walk(exports).sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.delete(path);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            });
        }
    }

    @Test
    void shouldCreateXmlFile() throws IOException {
        ProductMasterDto dto = buildDto();

        xmlExporter.export(dto);

        Path file = Path.of("exports", "P001.xml");

        assertTrue(Files.exists(file));
    }

    @Test
    void shouldCreateExportsDirectoryIfMissing() throws IOException {
        ProductMasterDto dto = buildDto();

        xmlExporter.export(dto);

        Path exportsDir = Path.of("exports");

        assertTrue(Files.exists(exportsDir));
        assertTrue(Files.isDirectory(exportsDir));
    }

    @Test
    void shouldWriteExpectedContent() throws Exception {
        ProductMasterDto dto = buildDto();

        xmlExporter.export(dto);

        Path file = Path.of("exports", "P001.xml");

        String xml = Files.readString(file);

        assertTrue(xml.contains("<productId>P001</productId>"));
        assertTrue(xml.contains("<sku>KB001</sku>"));
        assertTrue(xml.contains("<name>Keyboard</name>"));
        assertTrue(xml.contains("<currency>EUR</currency>"));
    }

    @Test
    void shouldOverwriteExistingFile() throws Exception {
        ProductMasterDto first =
                ProductMasterDto.builder().productId("P001").name("Keyboard").build();

        ProductMasterDto second = ProductMasterDto.builder()
                .productId("P001")
                .name("Mechanical Keyboard")
                .build();

        xmlExporter.export(first);
        xmlExporter.export(second);

        Path file = Path.of("exports", "P001.xml");

        String xml = Files.readString(file);

        assertTrue(xml.contains("Mechanical Keyboard"));
        assertFalse(xml.contains(">Keyboard<"));
    }

    private ProductMasterDto buildDto() {
        return ProductMasterDto.builder()
                .productId("P001")
                .sku("KB001")
                .name("Keyboard")
                .brand("Microsoft")
                .category("Accessories")
                .active(true)
                .price(BigDecimal.valueOf(49.99))
                .currency("EUR")
                .availableStock(120)
                .reservedStock(10)
                .supplierId("SUP001")
                .supplierName("TechSupplier")
                .supplierCountry("France")
                .shortDescription("Wireless Keyboard")
                .longDescription("Ergonomic wireless keyboard")
                .createdAt(Instant.parse("2026-08-26T09:00:00Z"))
                .lastUpdatedAt(Instant.parse("2026-08-26T09:04:00Z"))
                .build();
    }
}
