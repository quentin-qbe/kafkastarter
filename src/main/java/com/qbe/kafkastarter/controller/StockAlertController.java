package com.qbe.kafkastarter.controller;

import com.qbe.avro.StockAlertAction;
import com.qbe.kafkastarter.constants.TopicsConstants;
import com.qbe.kafkastarter.dto.StockAlertActionDto;
import com.qbe.kafkastarter.dto.StockAlertStateDto;
import com.qbe.kafkastarter.mapper.StockAlertActionMapper;
import com.qbe.kafkastarter.mapper.StockAlertStateMapper;
import com.qbe.kafkastarter.service.StockAlertService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/stock-alerts")
@RequiredArgsConstructor
@Slf4j
@Tag(
        name = "Stock Alerts",
        description =
                """
                        Exposes stock alert information stored in the Kafka Streams State Store.

                        The state store keeps track of products whose inventory
                        level has reached configured alert thresholds and provides
                        a queryable view through this REST API.
                        """)
public class StockAlertController {

    private final StockAlertService stockAlertService;
    private final KafkaTemplate<String, StockAlertAction> kafkaTemplate;
    private final StockAlertStateMapper stockAlertStateMapper;
    private final StockAlertActionMapper stockAlertActionMapper;

    @Operation(
            summary = "Retrieve all stock alerts",
            description =
                    """
                            Returns all stock alert states currently available
                            in the Kafka Streams State Store.

                            Useful for monitoring products currently under
                            stock surveillance or alert conditions.
                            """,
            responses = {
                @ApiResponse(responseCode = "200", description = "List of stock alert states returned successfully")
            })
    @GetMapping
    public List<StockAlertStateDto> getAllAlerts() {
        return stockAlertService.findAll().stream()
                .map(stockAlertStateMapper::fromAvro)
                .toList();
    }

    @Operation(
            summary = "Retrieve stock alert for a product",
            description =
                    """
                            Returns the current stock alert state for a specific product.

                            The product identifier is used as the key in the Kafka Streams
                            State Store and allows querying the latest alert information.
                            """,
            responses = {
                @ApiResponse(
                        responseCode = "200",
                        description = "Stock alert found",
                        content =
                                @Content(
                                        examples =
                                                @ExampleObject(
                                                        name = "Active Alert",
                                                        value =
                                                                """
                                                {
                                                  "alertActive": true,
                                                  "severity": "CRITICAL",
                                                  "lastKnownStock": 3,
                                                  "lastAlertDate": "2026-08-31T14:25:30Z"
                                                }
                                                """))),
                @ApiResponse(responseCode = "404", description = "No alert state found for the specified product")
            })
    @GetMapping("/{productId}")
    public ResponseEntity<StockAlertStateDto> getAlertByProductId(
            @Parameter(description = "Unique product identifier", example = "P001") @PathVariable String productId) {

        return Optional.ofNullable(stockAlertService.findByProductId(productId))
                .map(stockAlertStateMapper::fromAvro)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @Operation(
            summary = "Publish an alert action",
            description =
                    """
                    Publishes a StockAlertActionDto into topic-stock-alert-actions.
                    The productId is used as the Kafka message key, allowing
                    Kafka Streams to correlate and aggregate product data
                    across multiple topics.
                    """,
            responses = {
                @ApiResponse(responseCode = "202", description = "Stock alert action accepted and published to Kafka"),
                @ApiResponse(responseCode = "400", description = "Invalid request payload")
            })
    @PatchMapping
    public ResponseEntity<StockAlertActionDto> acknowledgeByProductId(
            @io.swagger.v3.oas.annotations.parameters.RequestBody(
                            description = "Alert action",
                            required = true,
                            content =
                                    @Content(
                                            examples =
                                                    @ExampleObject(
                                                            name = "action",
                                                            value =
                                                                    """
                            {
                              "eventId": "event-001",
                              "productId": "P001",
                              "action": "ACKNOWLEDGE",
                              "user": "user",
                              "processedAt": "2026-09-01T09:00:00Z"
                            }
                            """)))
                    @Valid
                    @RequestBody
                    StockAlertActionDto dto) {

        try {
            StockAlertAction event = stockAlertActionMapper.toAvro(dto);

            kafkaTemplate
                    .send(TopicsConstants.TOPIC_STOCK_ALERT_ACTIONS, dto.productId(), event)
                    .get();

            log.info(
                    "Stock alert action published for product {} on topic {}",
                    dto.productId(),
                    TopicsConstants.TOPIC_STOCK_ALERT_ACTIONS);
            return ResponseEntity.status(HttpStatus.ACCEPTED).body(dto);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while publishing stock alert action", e);
            return ResponseEntity.internalServerError().build();
        } catch (ExecutionException e) {
            log.error("Error while publishing stock alert action", e);
            return ResponseEntity.internalServerError().build();
        }
    }
}
