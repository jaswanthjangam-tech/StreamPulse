package io.streampulse.common;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;

/**
 * The event contract shared by the producer (order-api) and the consumer
 * (order-processor).
 * Serialized as JSON on topic {@code orders.v1}, keyed by customerId so one
 * customer's events
 * stay ordered inside a partition.
 *
 * <p>
 * Every field is required. The check lives here, in the constructor, so that a
 * malformed
 * message such as {@code {"foo":1}} fails while it is being parsed. The
 * consumer then
 * dead-letters just that one message. Without the check it would reach the
 * database as nulls,
 * fail the NOT NULL columns and take down the whole batch of 500 with it.
 *
 * <p>
 * The topic name carries the version ({@code orders.v1}). A breaking change to
 * this record
 * means a new topic ({@code orders.v2}), never a silent edit.
 */
public record OrderEvent(
        String eventId,
        String orderId,
        String customerId,
        String region,
        String category,
        BigDecimal amount,
        String status,
        Instant createdAt) {

    public OrderEvent {
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(orderId, "orderId");
        Objects.requireNonNull(customerId, "customerId");
        Objects.requireNonNull(region, "region");
        Objects.requireNonNull(category, "category");
        Objects.requireNonNull(amount, "amount");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(createdAt, "createdAt");
    }
}
