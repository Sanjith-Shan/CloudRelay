package com.cloudrelay.analytics;

import com.cloudrelay.dto.SessionResponse;
import com.cloudrelay.event.SessionEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The rule under test: a Kafka problem must never become a player's problem.
 *
 * <p>Each of these drives {@link KafkaAnalyticsSink} through a different broker
 * failure and asserts the same thing — the caller comes back normally — plus
 * that the failure was counted, because a tee that fails silently is only
 * marginally better than one that fails loudly.
 */
class KafkaAnalyticsSinkTest {

    private MeterRegistry meterRegistry;
    private ObjectMapper mapper;
    private KafkaAnalyticsSink sink;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        mapper = new ObjectMapper().registerModule(new JavaTimeModule())
                .disable(com.fasterxml.jackson.databind.SerializationFeature
                        .WRITE_DATES_AS_TIMESTAMPS);
    }

    @AfterEach
    void tearDown() {
        if (sink != null) {
            sink.shutdown();
        }
    }

    private SessionEvent event(String code) {
        return SessionEvent.builder()
                .eventId("evt-" + code)
                .eventTime(Instant.parse("2026-08-27T12:00:00Z"))
                .type("SESSION_CREATED")
                .sessionCode(code)
                .schemaVersion(SessionEvent.SCHEMA_VERSION)
                .session(SessionResponse.builder().sessionCode(code).gameId("g").build())
                .build();
    }

    @SuppressWarnings("unchecked")
    private KafkaTemplate<String, String> template() {
        return mock(KafkaTemplate.class);
    }

    private double counter(String name) {
        return meterRegistry.get(name).counter().count();
    }

    @Test
    void publishesEventsAndCountsThem() {
        KafkaTemplate<String, String> template = template();
        when(template.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));
        sink = new KafkaAnalyticsSink(template, mapper, "topic", 100, 1, meterRegistry);

        sink.accept(event("ABCD1234"));

        await().atMost(2, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(counter("cloudrelay.analytics.events.published"))
                        .isEqualTo(1));
    }

    @Test
    void aBrokerThatRejectsTheSendDoesNotThrowAtTheCaller() {
        KafkaTemplate<String, String> template = template();
        when(template.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.failedFuture(
                        new IllegalStateException("broker unreachable")));
        sink = new KafkaAnalyticsSink(template, mapper, "topic", 100, 1, meterRegistry);

        sink.accept(event("ABCD1234"));

        await().atMost(2, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(counter("cloudrelay.analytics.events.failed"))
                        .isEqualTo(1));
        assertThat(counter("cloudrelay.analytics.events.published")).isZero();
    }

    @Test
    void aBrokerThatBlocksTheSendDoesNotThrowAtTheCaller() {
        // send() throwing synchronously is what a producer does when it cannot
        // fetch cluster metadata within max.block.ms.
        KafkaTemplate<String, String> template = template();
        when(template.send(anyString(), anyString(), anyString()))
                .thenThrow(new org.apache.kafka.common.errors.TimeoutException(
                        "Topic metadata not available"));
        sink = new KafkaAnalyticsSink(template, mapper, "topic", 100, 1, meterRegistry);

        sink.accept(event("ABCD1234"));

        await().atMost(2, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(counter("cloudrelay.analytics.events.failed"))
                        .isEqualTo(1));
    }

    @Test
    void acceptDoesNotBlockTheCallingThread() throws Exception {
        // A producer stuck on an unreachable broker holds its caller for
        // max.block.ms. If accept() ran the send inline, that would be a
        // player's HTTP request parked for seconds.
        CountDownLatch senderIsStuck = new CountDownLatch(1);
        CountDownLatch releaseSender = new CountDownLatch(1);

        KafkaTemplate<String, String> template = template();
        when(template.send(anyString(), anyString(), anyString())).thenAnswer(invocation -> {
            senderIsStuck.countDown();
            releaseSender.await(5, TimeUnit.SECONDS);
            return CompletableFuture.completedFuture(null);
        });
        sink = new KafkaAnalyticsSink(template, mapper, "topic", 100, 1, meterRegistry);

        long start = System.nanoTime();
        sink.accept(event("ABCD1234"));
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertThat(senderIsStuck.await(2, TimeUnit.SECONDS))
                .as("the send should have started on a background thread")
                .isTrue();
        assertThat(elapsedMs)
                .as("accept() must return immediately even while the sender is stuck")
                .isLessThan(500);

        releaseSender.countDown();
    }

    @Test
    void aFullQueueDiscardsEventsAndCountsTheDrop() throws Exception {
        // One sender thread, a queue of one, and a send that never finishes:
        // everything after the first two events has nowhere to go.
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger sends = new AtomicInteger();

        KafkaTemplate<String, String> template = template();
        when(template.send(anyString(), anyString(), anyString())).thenAnswer(invocation -> {
            sends.incrementAndGet();
            release.await(5, TimeUnit.SECONDS);
            return CompletableFuture.completedFuture(null);
        });
        sink = new KafkaAnalyticsSink(template, mapper, "topic", 1, 1, meterRegistry);

        for (int i = 0; i < 50; i++) {
            sink.accept(event("CODE" + i));
        }

        assertThat(sink.droppedCount())
                .as("backpressure should shed events rather than queue without limit")
                .isGreaterThan(0);
        assertThat(counter("cloudrelay.analytics.events.dropped"))
                .isEqualTo(sink.droppedCount());

        release.countDown();
    }

    @Test
    void anEventThatCannotBeSerialisedIsCountedNotThrown() {
        ObjectMapper broken = mock(ObjectMapper.class);
        try {
            when(broken.writeValueAsString(any()))
                    .thenThrow(new com.fasterxml.jackson.core.JsonProcessingException("nope") {});
        } catch (Exception ignored) {
            // Stubbing a checked-throwing method; the compiler needs the catch.
        }
        sink = new KafkaAnalyticsSink(template(), broken, "topic", 100, 1, meterRegistry);

        sink.accept(event("ABCD1234"));

        await().atMost(2, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(counter("cloudrelay.analytics.events.failed"))
                        .isEqualTo(1));
    }

    @Test
    void reportsThatItIsEnabled() {
        sink = new KafkaAnalyticsSink(template(), mapper, "topic", 100, 1, meterRegistry);
        assertThat(sink.isEnabled()).isTrue();
        assertThat(new DisabledAnalyticsSink().isEnabled()).isFalse();
    }
}
