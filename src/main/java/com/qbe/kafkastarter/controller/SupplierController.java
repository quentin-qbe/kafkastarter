package com.qbe.kafkastarter.controller;

import com.qbe.avro.Supplier;
import com.qbe.kafkastarter.constants.TopicsConstants;
import com.qbe.kafkastarter.dto.SupplierDto;
import com.qbe.kafkastarter.mapper.SupplierMapper;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import java.util.concurrent.ExecutionException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

@RestController
@RequiredArgsConstructor
@Slf4j
@RequestMapping("/suppliers")
@Tag(
        name = "Suppliers",
        description =
                """
                Publishes supplier events into Kafka.

                Supplier information is joined with product,
                pricing, stock and marketing data through Kafka Streams
                to build the Product Master view.
                """)
public class SupplierController {

    private final KafkaTemplate<String, Supplier> kafkaTemplate;
    private final SupplierMapper supplierMapper;

    @Operation(
            summary = "Publish supplier information",
            description =
                    """
                    Publishes a supplier event into topic-supplier.
                    The productId is used as the Kafka message key,
                    allowing Kafka Streams to enrich the consolidated
                    Product Master view with supplier metadata.
                    """,
            responses = {
                @ApiResponse(responseCode = "201", description = "Supplier event created and published to Kafka"),
                @ApiResponse(responseCode = "400", description = "Invalid request payload")
            })
    @PostMapping
    public ResponseEntity<SupplierDto> createSupplier(
            @io.swagger.v3.oas.annotations.parameters.RequestBody(
                            description = "Supplier information associated with a product",
                            required = true,
                            content =
                                    @Content(
                                            examples =
                                                    @ExampleObject(
                                                            name = "Keyboard Supplier",
                                                            value =
                                                                    """
                    {
                      "productId": "P001",
                      "supplierId": "SUP001",
                      "supplierName": "TechSupplier",
                      "supplierCountry": "France",
                      "lastUpdatedAt": "2026-08-27T09:00:00Z"
                    }
                    """)))
                    @Valid
                    @RequestBody
                    SupplierDto dto) {

        try {
            Supplier supplier = supplierMapper.toAvro(dto);

            kafkaTemplate
                    .send(TopicsConstants.TOPIC_SUPPLIER, dto.productId(), supplier)
                    .get();

            log.info(
                    "Supplier event published for product {} on topic {}",
                    dto.productId(),
                    TopicsConstants.TOPIC_SUPPLIER);

            return ResponseEntity.status(HttpStatus.CREATED).body(dto);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while publishing supplier event", e);
            return ResponseEntity.internalServerError().build();

        } catch (ExecutionException e) {
            log.error("Error while publishing supplier event", e);
            return ResponseEntity.internalServerError().build();
        }
    }
}
