package com.qbe.kafkastarter.service;

import com.qbe.kafkastarter.dto.ProductMasterDto;
import com.qbe.kafkastarter.exporter.CsvExporter;
import com.qbe.kafkastarter.exporter.XmlExporter;
import java.io.IOException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class ExportService {

    private final CsvExporter csvExporter;
    private final XmlExporter xmlExporter;

    public void exportCsvAndXml(ProductMasterDto dto) {
        try {
            csvExporter.export(dto);
            xmlExporter.export(dto);
            log.info("Product {} exported successfully", dto.productId());
        } catch (IOException e) {
            throw new RuntimeException("Failed to export product " + dto.productId(), e);
        }
    }
}
