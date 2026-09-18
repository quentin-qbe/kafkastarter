package com.qbe.kafkastarter.consumer;

import com.qbe.avro.ProductMaster;
import com.qbe.kafkastarter.constants.ConsumerConstants;
import com.qbe.kafkastarter.constants.TopicsConstants;
import com.qbe.kafkastarter.mapper.ProductMasterMapper;
import com.qbe.kafkastarter.service.ExportService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@RequiredArgsConstructor
@Component
public class ProductMasterConsumer {

    private final ExportService exporterService;
    private final ProductMasterMapper productMasterMapper;

    @KafkaListener(topics = TopicsConstants.TOPIC_MASTER_PRODUCT, groupId = ConsumerConstants.PRODUCT_MASTER_CONSUMER)
    public void consume(ProductMaster product) {
        exporterService.exportCsvAndXml(productMasterMapper.fromAvro(product));
    }
}
