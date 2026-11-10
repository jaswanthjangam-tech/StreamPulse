package io.streampulse.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins the wire format. order-api writes these JSON field names and
 * order-processor reads them.
 * The compiler already stops a renamed field from slipping through the Java
 * code, but it cannot
 * protect the bytes: Kafka keeps messages for 7 days, and during a rolling
 * update an old pod and
 * a new pod briefly share the topic. Pinning the JSON makes any change to it a
 * deliberate decision.
 */
class OrderEventContractTest {

    // Same settings Spring Boot applies to its ObjectMapper: ISO-8601 dates, not
    // epoch numbers.
    private final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private static OrderEvent sample() {
        return new OrderEvent("e-1", "o-1", "cust-42", "APAC", "toys",
                new BigDecimal("19.90"), "PAID", Instant.parse("2026-11-07T21:00:00Z"));
    }

    @Test
    void jsonUsesExactlyTheFieldNamesTheConsumerExpects() throws Exception {
        JsonNode json = mapper.readTree(mapper.writeValueAsString(sample()));

        List<String> names = new ArrayList<>();
        json.fieldNames().forEachRemaining(names::add);

        assertThat(names).containsExactlyInAnyOrder(
                "eventId", "orderId", "customerId", "region", "category", "amount", "status", "createdAt");
    }

    @Test
    void timestampIsIso8601AndAmountIsANumber() throws Exception {
        JsonNode json = mapper.readTree(mapper.writeValueAsString(sample()));

        assertThat(json.get("createdAt").asText()).isEqualTo("2026-11-07T21:00:00Z");
        assertThat(json.get("amount").isNumber()).isTrue();
        assertThat(json.get("amount").decimalValue()).isEqualByComparingTo("19.90");
    }

    @Test
    void roundTripLosesNothing() throws Exception {
        OrderEvent original = sample();

        OrderEvent copy = mapper.readValue(mapper.writeValueAsString(original), OrderEvent.class);

        assertThat(copy.eventId()).isEqualTo(original.eventId());
        assertThat(copy.orderId()).isEqualTo(original.orderId());
        assertThat(copy.customerId()).isEqualTo(original.customerId());
        assertThat(copy.region()).isEqualTo(original.region());
        assertThat(copy.category()).isEqualTo(original.category());
        assertThat(copy.amount()).isEqualByComparingTo(original.amount());
        assertThat(copy.status()).isEqualTo(original.status());
        assertThat(copy.createdAt()).isEqualTo(original.createdAt());
    }

    @Test
    void messageMissingRequiredFieldsFailsToParse() {
        assertThatThrownBy(() -> mapper.readValue("{\"eventId\":\"e-1\"}", OrderEvent.class))
                .isInstanceOf(JsonProcessingException.class);
        assertThatThrownBy(() -> mapper.readValue("{\"foo\":1}", OrderEvent.class))
                .isInstanceOf(JsonProcessingException.class);
    }

    @Test
    void constructorRejectsNullRequiredFields() {
        assertThatThrownBy(
                () -> new OrderEvent(null, "o-1", "c", "APAC", "toys", BigDecimal.ONE, "PAID", Instant.now()))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("eventId");
        assertThatThrownBy(() -> new OrderEvent("e", "o-1", "c", "APAC", "toys", null, "PAID", Instant.now()))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("amount");
    }
}
