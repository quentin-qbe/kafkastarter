package com.qbe.kafkastarter.exporter;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.qbe.kafkastarter.dto.ProductMasterDto;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class XmlExporter {

    private final XmlMapper xmlMapper;

    public void export(ProductMasterDto dto) throws IOException {
        Path file = Path.of("exports", dto.productId() + ".xml");
        Files.createDirectories(file.getParent());
        xmlMapper.writeValue(file.toFile(), dto);
        log.info("Exported product {} to XML", dto.productId());
    }
}
