package com.qbe.kafkastarter.controller;

import com.qbe.avro.Stock;
import com.qbe.kafkastarter.constants.TopicsConstants;
import com.qbe.kafkastarter.dto.StockDto;
import com.qbe.kafkastarter.mapper.StockMapper;
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
@RequestMapping("/stocks")
@Tag(
        name = "Stocks",
        description =
                """
                Publishes stock events into Kafka.

                Stock information is joined with product, pricing,
                supplier and marketing data through Kafka Streams
                to build the Product Master view.
                """)
public class StockController {

    private final KafkaTemplate<String, Stock> kafkaTemplate;
    private final StockMapper stockMapper;

    @Operation(
            summary = "Publish stock information",
            description =
                    """
                    Publishes a stock event into topic-stock.
                    The productId is used as the Kafka message key,
                    allowing Kafka Streams to aggregate inventory
                    information with other product-related events.
                    """,
            responses = {
                @ApiResponse(responseCode = "201", description = "Stock event created and published to Kafka"),
                @ApiResponse(responseCode = "400", description = "Invalid request payload")
            })
    @PostMapping
    public ResponseEntity<StockDto> createStock(
            @io.swagger.v3.oas.annotations.parameters.RequestBody(
                            description = "Stock information associated with a product",
                            required = true,
                            content =
                                    @Content(
                                            examples =
                                                    @ExampleObject(
                                                            name = "Keyboard Stock",
                                                            value =
                                                                    """
                                            {
                                              "productId": "P001",
                                              "availableQuantity": 50,
                                              "reservedQuantity": 0,
                                              "warehouseCode": "WH001",
                                              "lastUpdatedAt": "2026-09-01T07:30:00Z"
                                            }
                                            """)))
                    @Valid
                    @RequestBody
                    StockDto dto) {

        try {
            Stock stock = stockMapper.toAvro(dto);

            kafkaTemplate
                    .send(TopicsConstants.TOPIC_STOCK, dto.productId(), stock)
                    .get();

            log.info("Stock event published for product {} on topic {}", dto.productId(), TopicsConstants.TOPIC_STOCK);

            return ResponseEntity.status(HttpStatus.CREATED).body(dto);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while publishing stock event", e);
            return ResponseEntity.internalServerError().build();

        } catch (ExecutionException e) {
            log.error("Error while publishing stock event", e);
            return ResponseEntity.internalServerError().build();
        }
    }
}
