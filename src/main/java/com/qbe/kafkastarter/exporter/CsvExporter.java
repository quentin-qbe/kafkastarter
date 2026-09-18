package com.qbe.kafkastarter.exporter;

import com.qbe.kafkastarter.dto.ProductMasterDto;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class CsvExporter {

    public void export(ProductMasterDto dto) throws IOException {
        Path file = Path.of("exports", dto.productId() + ".csv");
        Files.createDirectories(file.getParent());

        String content =
                """
            productId;sku;name;price;currency;availableStock;updatedAt
            %s;%s;%s;%s;%s;%s;%s
            """
                        .formatted(
                                dto.productId(),
                                dto.sku(),
                                dto.name(),
                                dto.price(),
                                dto.currency(),
                                dto.availableStock(),
                                dto.lastUpdatedAt());

        Files.writeString(file, content, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        log.info("Exported product {} to CSV", dto.productId());
    }
}
