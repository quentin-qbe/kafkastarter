package com.qbe.kafkastarter.controller;

import com.qbe.avro.Product;
import com.qbe.kafkastarter.constants.TopicsConstants;
import com.qbe.kafkastarter.dto.ProductDto;
import com.qbe.kafkastarter.mapper.ProductMapper;
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
@RequestMapping("/products")
@Tag(
        name = "Products",
        description =
                """
                Publishes product lifecycle events into Kafka.

                Product information is later joined with pricing, stock,
                supplier and marketing data through Kafka Streams in order
                to build a consolidated product view.
                """)
public class ProductController {

    private final KafkaTemplate<String, Product> kafkaTemplate;
    private final ProductMapper productMapper;

    @Operation(
            summary = "Publish a product event",
            description =
                    """
                    Publishes a ProductDto into topic-product.
                    The productId is used as the Kafka message key, allowing
                    Kafka Streams to correlate and aggregate product data
                    across multiple topics.
                    """,
            responses = {
                @ApiResponse(responseCode = "201", description = "Product event created and published to Kafka"),
                @ApiResponse(responseCode = "400", description = "Invalid request payload")
            })
    @PostMapping
    public ResponseEntity<ProductDto> createProduct(
            @io.swagger.v3.oas.annotations.parameters.RequestBody(
                            description = "Product information",
                            required = true,
                            content =
                                    @Content(
                                            examples =
                                                    @ExampleObject(
                                                            name = "Keyboard",
                                                            value =
                                                                    """
                    {
                      "productId": "P001",
                      "sku": "KB001",
                      "name": "Keyboard",
                      "category": "Accessories",
                      "brand": "Microsoft",
                      "active": true,
                      "alertThreshold": 50,
                      "createdAt": "2026-08-27T08:00:00Z"
                    }
                    """)))
                    @Valid
                    @RequestBody
                    ProductDto dto) {

        try {
            Product product = productMapper.toAvro(dto);

            kafkaTemplate
                    .send(TopicsConstants.TOPIC_PRODUCT, dto.productId(), product)
                    .get();

            log.info(
                    "Product event published for product {} on topic {}",
                    dto.productId(),
                    TopicsConstants.TOPIC_PRODUCT);

            return ResponseEntity.status(HttpStatus.CREATED).body(dto);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while publishing product event", e);
            return ResponseEntity.internalServerError().build();

        } catch (ExecutionException e) {
            log.error("Error while publishing product event", e);
            return ResponseEntity.internalServerError().build();
        }
    }
}
