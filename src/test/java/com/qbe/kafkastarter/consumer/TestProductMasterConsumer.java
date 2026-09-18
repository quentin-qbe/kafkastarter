package com.qbe.kafkastarter.consumer;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.qbe.avro.ProductMaster;
import com.qbe.kafkastarter.dto.ProductMasterDto;
import com.qbe.kafkastarter.mapper.ProductMasterMapper;
import com.qbe.kafkastarter.service.ExportService;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TestProductMasterConsumer {

    @Mock
    private ExportService exporterService;

    @Mock
    private ProductMasterMapper productMasterMapper;

    @InjectMocks
    private ProductMasterConsumer productMasterConsumer;

    @Test
    void shouldExportMappedDto() {
        ProductMaster event = ProductMaster.newBuilder()
                .setProductId("P001")
                .setSku("KB001")
                .setName("Mechanical Keyboard")
                .setBrand("Microsoft")
                .setCategory("Accessories")
                .setActive(true)
                .setPrice(49.99)
                .setCurrency("EUR")
                .setAvailableStock(10)
                .setReservedStock(2)
                .setSupplierId("SUP001")
                .setSupplierName("TechSupplier")
                .setSupplierCountry("FR")
                .setShortDescription("Mechanical keyboard")
                .setLongDescription("High quality mechanical keyboard with RGB lighting")
                .setTags(List.of("keyboard", "gaming", "rgb"))
                .setAlertThreshold(20)
                .setCreatedAt(Instant.parse("2026-08-29T10:00:00Z"))
                .setLastUpdatedAt(Instant.parse("2026-09-02T13:00:00Z"))
                .build();

        ProductMasterDto dto = ProductMasterDto.builder()
                .productId("P001")
                .sku("KB001")
                .name("Mechanical Keyboard")
                .brand("Microsoft")
                .category("Accessories")
                .active(true)
                .price(BigDecimal.valueOf(49.99))
                .currency("EUR")
                .availableStock(10)
                .reservedStock(2)
                .supplierId("SUP001")
                .supplierName("TechSupplier")
                .supplierCountry("FR")
                .shortDescription("Mechanical keyboard")
                .longDescription("High quality mechanical keyboard with RGB lighting")
                .tags(List.of("keyboard", "gaming", "rgb"))
                .alertThreshold(20)
                .createdAt(Instant.parse("2026-08-29T10:00:00Z"))
                .lastUpdatedAt(Instant.parse("2026-09-02T13:00:00Z"))
                .build();

        when(productMasterMapper.fromAvro(event)).thenReturn(dto);

        productMasterConsumer.consume(event);

        verify(productMasterMapper).fromAvro(event);
        verify(exporterService).exportCsvAndXml(dto);
    }
}
