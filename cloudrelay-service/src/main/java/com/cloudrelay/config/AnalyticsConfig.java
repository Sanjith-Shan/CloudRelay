package com.cloudrelay.config;

import com.cloudrelay.analytics.AnalyticsEventSink;
import com.cloudrelay.analytics.AnalyticsProperties;
import com.cloudrelay.analytics.DisabledAnalyticsSink;
import com.cloudrelay.analytics.KafkaAnalyticsSink;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;

/**
 * Wires the analytics tee, or wires it out.
 *
 * <p>The service has exactly one {@link AnalyticsEventSink} bean either way, so
 * {@code SessionEventPublisher} has no branch in it and the tests do not need a
 * broker to exercise the publish path.
 */
@Configuration
@EnableConfigurationProperties(AnalyticsProperties.class)
public class AnalyticsConfig {

    /**
     * The wire format between the service and the lakehouse.
     *
     * <p>Deliberately its own mapper rather than the one Spring MVC uses for
     * HTTP responses. Changing how the API renders a timestamp would otherwise
     * silently change the format the Spark job parses, and that coupling is not
     * one anybody would think to look for.
     */
    @Bean
    public ObjectMapper analyticsObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.registerModule(millisecondInstants());
        mapper.disable(com.fasterxml.jackson.databind.SerializationFeature
                .WRITE_DATES_AS_TIMESTAMPS);
        return mapper;
    }

    /**
     * Writes every {@link Instant} at millisecond precision.
     *
     * <p>Jackson's default renders the full nanosecond value that
     * {@code Instant.now()} carries on Linux. Spark's string-to-timestamp cast
     * accepts at most six fractional digits and returns null beyond that, so a
     * nine digit timestamp does not fail loudly, it silently becomes an
     * unparseable event that the pipeline quarantines. Truncating at the
     * producer is where that belongs: the precision was never real information
     * for a session lifecycle event, and the alternative is a parsing rule in
     * Spark that has to guess.
     */
    private static SimpleModule millisecondInstants() {
        SimpleModule module = new SimpleModule("cloudrelay-millis");
        module.addSerializer(Instant.class, new JsonSerializer<>() {
            @Override
            public void serialize(Instant value, JsonGenerator gen, SerializerProvider serializers)
                    throws IOException {
                gen.writeString(value.truncatedTo(ChronoUnit.MILLIS).toString());
            }
        });
        return module;
    }

    @Bean
    @ConditionalOnProperty(prefix = "cloudrelay.analytics", name = "enabled",
            havingValue = "true")
    public AnalyticsEventSink kafkaAnalyticsSink(AnalyticsProperties properties,
                                                 ObjectMapper analyticsObjectMapper,
                                                 MeterRegistry meterRegistry) {
        return new KafkaAnalyticsSink(
                analyticsKafkaTemplate(properties),
                analyticsObjectMapper,
                properties.getTopic(),
                properties.getQueueCapacity(),
                properties.getSenderThreads(),
                meterRegistry);
    }

    @Bean
    @ConditionalOnProperty(prefix = "cloudrelay.analytics", name = "enabled",
            havingValue = "false", matchIfMissing = true)
    public AnalyticsEventSink disabledAnalyticsSink() {
        return new DisabledAnalyticsSink();
    }

    /**
     * Producer settings tuned for a best-effort tee rather than for a system of
     * record.
     *
     * <p>{@code acks=1} and idempotence off: the leader acknowledging is enough
     * for analytics, and waiting for the full ISR would add latency to protect
     * data that the pipeline can already survive losing. Turning idempotence off
     * is not an oversight — it is required, because the idempotent producer
     * demands {@code acks=all}. The cost is that a retry can duplicate an event,
     * which is precisely the case the {@code eventId} deduplication in silver
     * exists to absorb. The duplicate is handled once, downstream, instead of
     * being paid for on every send.
     *
     * <p>{@code max.block.ms} is short because a broker that is not answering
     * should surface as a dropped event within seconds, not as sender threads
     * parked for a minute on metadata that is not coming.
     *
     * <p>Public rather than private so that {@code AnalyticsTeeIT} drives a real
     * broker through exactly these settings. An integration test that builds its
     * own producer config is testing its own config.
     */
    public KafkaTemplate<String, String> analyticsKafkaTemplate(AnalyticsProperties properties) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, "1");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 2000);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 5000);
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 2000);
        config.put(ProducerConfig.RETRIES_CONFIG, 2);
        // Batch aggressively. 20ms of latency is nothing to a table that gets
        // queried by the minute, and it is the difference between one request
        // per event and one per batch at 700 events a second.
        config.put(ProducerConfig.LINGER_MS_CONFIG, 20);
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        ProducerFactory<String, String> factory = new DefaultKafkaProducerFactory<>(config);
        return new KafkaTemplate<>(factory);
    }
}
