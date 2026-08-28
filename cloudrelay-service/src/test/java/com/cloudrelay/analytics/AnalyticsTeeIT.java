package com.cloudrelay.analytics;

import com.cloudrelay.config.AnalyticsConfig;
import com.cloudrelay.dto.SessionResponse;
import com.cloudrelay.event.SessionEvent;
import com.cloudrelay.event.SessionEventPublisher;
import com.cloudrelay.model.Player;
import com.cloudrelay.model.SessionState;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.RedisTemplate;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;

/**
 * The analytics tee against a real broker, and then against one that is gone.
 *
 * <p>The unit tests prove the sink's contract with mocks. What they cannot prove
 * is that a real {@code KafkaProducer} configured the way this service
 * configures it behaves the way the contract assumes — in particular, that
 * {@code max.block.ms} really does bound how long a send can hold a thread when
 * there is no broker. That is the claim the whole design rests on, so it is
 * worth testing against the real thing.
 */
@Testcontainers(disabledWithoutDocker = true)
class AnalyticsTeeIT {

    private static final String TOPIC = "cloudrelay.session-events";

    @Container
    private static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    private final AnalyticsConfig config = new AnalyticsConfig();
    private final ObjectMapper mapper = config.analyticsObjectMapper();
    private final MeterRegistry meterRegistry = new SimpleMeterRegistry();
    private KafkaAnalyticsSink sink;

    @AfterEach
    void tearDown() {
        if (sink != null) {
            sink.shutdown();
            sink = null;
        }
    }

    private AnalyticsProperties properties(String bootstrapServers) {
        AnalyticsProperties properties = new AnalyticsProperties();
        properties.setEnabled(true);
        properties.setBootstrapServers(bootstrapServers);
        properties.setTopic(TOPIC);
        properties.setQueueCapacity(500);
        properties.setSenderThreads(2);
        return properties;
    }

    private KafkaAnalyticsSink sinkFor(String bootstrapServers) {
        AnalyticsProperties properties = properties(bootstrapServers);
        return new KafkaAnalyticsSink(
                config.analyticsKafkaTemplate(properties),
                mapper,
                properties.getTopic(),
                properties.getQueueCapacity(),
                properties.getSenderThreads(),
                meterRegistry);
    }

    private SessionResponse session(String code) {
        Player host = Player.builder()
                .playerId("player-1").displayName("Alice").region("us-west-2")
                .joinedAt(Instant.now()).connected(true).build();
        return SessionResponse.builder()
                .id(code + "-id").sessionCode(code).gameId("cyberpunk-2077")
                .region("us-west-2").state(SessionState.WAITING)
                .host(host).players(new ArrayList<>(List.of(host)))
                .maxPlayers(4).minPlayersToStart(2)
                .createdAt(Instant.now()).updatedAt(Instant.now())
                .currentPlayerCount(1).isFull(false).uptimeSeconds(0)
                .build();
    }

    private SessionEvent event(String code) {
        return SessionEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .eventTime(Instant.now())
                .type("SESSION_CREATED")
                .sessionCode(code)
                .schemaVersion(SessionEvent.SCHEMA_VERSION)
                .session(session(code))
                .build();
    }

    private List<ConsumerRecord<String, String>> drain(int expected) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "it-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        List<ConsumerRecord<String, String>> collected = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(TOPIC));
            long deadline = System.currentTimeMillis() + 30_000;
            while (collected.size() < expected && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(500));
                records.forEach(collected::add);
            }
        }
        return collected;
    }

    @Test
    void eventsReachTheBrokerAsParseableJson() throws Exception {
        sink = sinkFor(KAFKA.getBootstrapServers());

        sink.accept(event("REALKAF1"));

        List<ConsumerRecord<String, String>> records = drain(1);
        assertThat(records).hasSize(1);

        ConsumerRecord<String, String> record = records.get(0);
        assertThat(record.key())
                .as("keyed by session code so a session's events keep their order")
                .isEqualTo("REALKAF1");

        JsonNode json = mapper.readTree(record.value());
        assertThat(json.get("sessionCode").asText()).isEqualTo("REALKAF1");
        assertThat(json.get("eventId").asText()).isNotBlank();
        assertThat(json.get("type").asText()).isEqualTo("SESSION_CREATED");
        assertThat(json.get("session").get("gameId").asText()).isEqualTo("cyberpunk-2077");

        assertThat(meterRegistry.get("cloudrelay.analytics.events.published")
                .counter().count()).isEqualTo(1);
    }

    @Test
    void aBurstOfEventsAllArrive() {
        sink = sinkFor(KAFKA.getBootstrapServers());
        int burst = 300;

        for (int i = 0; i < burst; i++) {
            sink.accept(event(String.format("BURST%03d", i)));
        }

        await().atMost(45, TimeUnit.SECONDS).untilAsserted(() ->
                assertThat(meterRegistry.get("cloudrelay.analytics.events.published")
                        .counter().count()).isEqualTo(burst));
        assertThat(sink.droppedCount())
                .as("a queue of 500 should absorb a burst of 300 without shedding")
                .isZero();
    }

    /**
     * The claim the design exists to support, tested against a real producer.
     *
     * <p>Port 1 has nothing listening, so the producer cannot fetch metadata and
     * every send blocks for {@code max.block.ms}. If {@code accept} did the send
     * inline, each of these calls would be a player's HTTP request held for two
     * seconds; on the background executor it is a background thread's problem
     * and the caller does not notice.
     */
    @Test
    void aBrokerThatIsNotThereNeverReachesTheCaller() {
        sink = sinkFor("localhost:1");
        SessionEventPublisher publisher = new SessionEventPublisher(
                mockRedis(), sink);

        long start = System.nanoTime();
        for (int i = 0; i < 200; i++) {
            int index = i;
            assertThatCode(() -> publisher.publish("SESSION_CREATED",
                    session(String.format("DEAD%04d", index))))
                    .doesNotThrowAnyException();
        }
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertThat(elapsedMs)
                .as("200 publishes against a dead broker should cost the caller almost nothing")
                .isLessThan(2_000);

        // The events did not vanish quietly. Either the send failed or the
        // queue shed it, and both are counted.
        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() ->
                assertThat(meterRegistry.get("cloudrelay.analytics.events.failed")
                        .counter().count() + sink.droppedCount())
                        .isGreaterThan(0));
    }

    @SuppressWarnings("unchecked")
    private RedisTemplate<String, Object> mockRedis() {
        return mock(RedisTemplate.class);
    }
}
