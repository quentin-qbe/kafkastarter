package com.qbe.kafkastarter.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.qbe.kafkastarter.dto.ProductMasterDto;
import com.qbe.kafkastarter.exporter.CsvExporter;
import com.qbe.kafkastarter.exporter.XmlExporter;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TestExportService {

    @Mock
    private CsvExporter csvExporter;

    @Mock
    private XmlExporter xmlExporter;

    @InjectMocks
    private ExportService exportService;

    @Test
    void shouldExportCsvAndXml() throws Exception {
        ProductMasterDto dto = ProductMasterDto.builder().productId("P001").build();

        exportService.exportCsvAndXml(dto);

        verify(csvExporter).export(dto);
        verify(xmlExporter).export(dto);
    }

    @Test
    void shouldThrowRuntimeExceptionWhenCsvExportFails() throws Exception {
        ProductMasterDto dto = buildProductMasterDto();

        IOException cause = new IOException("CSV error");
        doThrow(cause).when(csvExporter).export(dto);

        RuntimeException exception = assertThrows(RuntimeException.class, () -> exportService.exportCsvAndXml(dto));

        assertEquals("Failed to export product P001", exception.getMessage());
        assertEquals(cause, exception.getCause());

        verify(csvExporter).export(dto);
        verifyNoInteractions(xmlExporter);
    }

    @Test
    void shouldThrowRuntimeExceptionWhenXmlExportFails() throws Exception {
        ProductMasterDto dto = ProductMasterDto.builder().productId("P001").build();

        IOException cause = new IOException("XML error");
        doThrow(cause).when(xmlExporter).export(dto);
        RuntimeException exception = assertThrows(RuntimeException.class, () -> exportService.exportCsvAndXml(dto));

        assertEquals("Failed to export product P001", exception.getMessage());
        assertEquals(cause, exception.getCause());

        verify(csvExporter).export(dto);
        verify(xmlExporter).export(dto);
    }

    private ProductMasterDto buildProductMasterDto() {

        Instant createdAt = Instant.parse("2026-08-26T09:00:00Z");
        Instant updatedAt = Instant.parse("2026-08-26T09:04:00Z");

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
                .tags(List.of("office", "wireless"))
                .createdAt(createdAt)
                .lastUpdatedAt(updatedAt)
                .build();
    }
}
