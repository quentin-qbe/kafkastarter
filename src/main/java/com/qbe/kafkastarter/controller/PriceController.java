package com.qbe.kafkastarter.controller;

import com.qbe.avro.Price;
import com.qbe.kafkastarter.constants.TopicsConstants;
import com.qbe.kafkastarter.dto.PriceDto;
import com.qbe.kafkastarter.mapper.PriceMapper;
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
@RequestMapping("/prices")
@Tag(
        name = "Prices",
        description =
                """
                Publishes product pricing information into Kafka.
                These events are consumed by Kafka Streams and aggregated
                into the Product Master topic.
                """)
public class PriceController {

    private final KafkaTemplate<String, Price> kafkaTemplate;
    private final PriceMapper priceMapper;

    @Operation(
            summary = "Publish a product price",
            description =
                    """
                    Publishes a pricing event into topic-price.
                    The productId is used as the Kafka message key to allow
                    Kafka Streams to join price data with product, stock,
                    supplier and marketing information.
                    """,
            responses = {
                @ApiResponse(responseCode = "201", description = "Price event created and published to Kafka"),
                @ApiResponse(responseCode = "400", description = "Invalid request payload")
            })
    @PostMapping
    public ResponseEntity<PriceDto> createPrice(
            @io.swagger.v3.oas.annotations.parameters.RequestBody(
                            description = "Price information associated with a product",
                            required = true,
                            content =
                                    @Content(
                                            examples =
                                                    @ExampleObject(
                                                            name = "Keyboard Price",
                                                            value =
                                                                    """
                    {
                      "productId": "P001",
                      "amount": 49.99,
                      "currency": "EUR",
                      "lastUpdatedAt": "2026-08-27T09:00:00Z"
                    }
                    """)))
                    @Valid
                    @RequestBody
                    PriceDto dto) {
        try {
            Price price = priceMapper.toAvro(dto);
            kafkaTemplate
                    .send(TopicsConstants.TOPIC_PRICE, dto.productId(), price)
                    .get();

            log.info("Price event published for product {} on topic {}", dto.productId(), TopicsConstants.TOPIC_PRICE);
            return ResponseEntity.status(HttpStatus.CREATED).body(dto);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while publishing price event", e);
            return ResponseEntity.internalServerError().build();
        } catch (ExecutionException e) {
            log.error("Error while publishing price event", e);
            return ResponseEntity.internalServerError().build();
        }
    }
}
