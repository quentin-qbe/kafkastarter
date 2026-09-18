package com.qbe.kafkastarter.controller;

import com.qbe.avro.Marketing;
import com.qbe.kafkastarter.constants.TopicsConstants;
import com.qbe.kafkastarter.dto.MarketingDto;
import com.qbe.kafkastarter.mapper.MarketingMapper;
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
@RequestMapping("/marketing")
@Tag(name = "Marketing", description = "Publish marketing events into Kafka topic topic-marketing")
public class MarketingController {

    private final KafkaTemplate<String, Marketing> kafkaTemplate;
    private final MarketingMapper marketingMapper;

    @Operation(
            summary = "Publish marketing information",
            description = "Publishes a marketing event into Kafka topic 'topic-marketing'. "
                    + "The event will later be aggregated into ProductMasterDto via Kafka Streams.",
            responses = {
                @ApiResponse(responseCode = "201", description = "Marketing event created"),
                @ApiResponse(responseCode = "400", description = "Invalid payload")
            })
    @PostMapping
    public ResponseEntity<MarketingDto> createMarketing(
            @io.swagger.v3.oas.annotations.parameters.RequestBody(
                            description = "Marketing information for a product",
                            required = true,
                            content =
                                    @Content(
                                            examples =
                                                    @ExampleObject(
                                                            value =
                                                                    """
                                            {
                                              "productId": "P001",
                                              "shortDescription": "Wireless Keyboard",
                                              "longDescription": "Ergonomic wireless keyboard",
                                              "tags": [
                                                "office",
                                                "wireless"
                                              ],
                                              "lastUpdatedAt": "2026-08-27T08:00:00Z"
                                            }
                                            """)))
                    @Valid
                    @RequestBody
                    MarketingDto dto) {
        try {
            Marketing marketing = marketingMapper.toAvro(dto);
            kafkaTemplate
                    .send(TopicsConstants.TOPIC_MARKETING, dto.productId(), marketing)
                    .get();
            log.info("Marketing event published for product {}", dto.productId());
            return ResponseEntity.status(HttpStatus.CREATED).body(dto);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while publishing marketing event", e);
            return ResponseEntity.internalServerError().build();
        } catch (ExecutionException e) {
            log.error("Error while publishing marketing event", e);
            return ResponseEntity.internalServerError().build();
        }
    }
}
