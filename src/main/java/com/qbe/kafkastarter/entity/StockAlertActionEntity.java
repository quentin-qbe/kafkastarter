package com.qbe.kafkastarter.entity;

import com.qbe.kafkastarter.enums.AlertActionType;
import jakarta.persistence.*;
import java.time.Instant;
import lombok.*;

@Entity
@Table(
        name = "stock_alert_action_history",
        uniqueConstraints = {@UniqueConstraint(name = "uk_stock_alert_action_event_id", columnNames = "event_id")})
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class StockAlertActionEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id", nullable = false)
    private Long id;

    @Column(name = "event_id", nullable = false, unique = true, updatable = false)
    private String eventId;

    @Column(name = "product_id", nullable = false, length = 255)
    private String productId;

    @Enumerated(EnumType.STRING)
    @Column(name = "action", nullable = false, length = 50)
    private AlertActionType action;

    @Column(name = "username", nullable = false, length = 255)
    private String username;

    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;
}
