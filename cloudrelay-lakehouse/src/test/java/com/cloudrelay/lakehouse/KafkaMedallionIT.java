package com.cloudrelay.lakehouse;

import com.cloudrelay.lakehouse.aggregate.GoldAggregates;
import com.cloudrelay.lakehouse.ingest.BronzeIngestion;
import com.cloudrelay.lakehouse.ingest.KafkaEventSource;
import com.cloudrelay.lakehouse.transform.SilverTransform;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.spark.sql.functions.sum;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The pipeline against a real broker.
 *
 * <p>Everything else in this module runs off the file source, which is fast,
 * deterministic and does not need Docker. What it cannot exercise is the part
 * that is specific to Kafka: offsets, partitions, keying, and the fact that
 * Spark's Kafka source and the checkpoint have to agree about where the stream
 * got to. Those are worth one slower test that runs a genuine broker.
 */
@Testcontainers(disabledWithoutDocker = true)
// Ordered on purpose. Starting a broker and running three streaming stages
// costs a minute, so these tests share one pipeline run rather than each
// building their own. That makes them a sequence, not a set: the last two
// publish more events, and the assertions about the original batch have to be
// made before that happens. The alternative is a clean broker per test and a
// suite that takes ten minutes.
@org.junit.jupiter.api.TestMethodOrder(
        org.junit.jupiter.api.MethodOrderer.OrderAnnotation.class)
class KafkaMedallionIT extends SparkTestBase {

    private static final int SESSIONS = 200;

    @Container
    private static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    private static LakehouseOptions options;
    private static List<String> produced;

    @BeforeAll
    static void produceAndRun() throws Exception {
        options = LakehouseOptions.builder()
                .basePath(freshLakehouse("kafka").basePath())
                .kafkaBootstrapServers(KAFKA.getBootstrapServers())
                .kafkaTopic("cloudrelay.session-events")
                .shufflePartitions(2)
                .sparkMaster("local[2]")
                .build();

        produced = TestEvents.lines(SESSIONS, Instant.parse("2026-08-27T09:00:00Z"), 77L);
        publish(produced);

        new BronzeIngestion(options, new KafkaEventSource(options)).runOnce(spark);
        new SilverTransform(options).runOnce(spark);
        new GoldAggregates(options).runOnce(spark);
    }

    /** Keyed by session code, exactly as {@code KafkaAnalyticsSink} does. */
    private static void publish(List<String> events) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        Pattern sessionCode = Pattern.compile("\"sessionCode\":\"([^\"]+)\"");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (String event : events) {
                Matcher m = sessionCode.matcher(event);
                String key = m.find() ? m.group(1) : null;
                producer.send(new ProducerRecord<>(options.kafkaTopic(), key, event));
            }
            producer.flush();
        }
    }

    @org.junit.jupiter.api.Order(1)
    @Test
    void everyPublishedEventReachesBronze() {
        assertThat(DeltaTables.read(spark, options.bronzePath()).count())
                .isEqualTo(produced.size());
    }

    @org.junit.jupiter.api.Order(2)
    @Test
    void bronzeRecordsTheKafkaCoordinatesOfEveryRow() {
        // The offset is what makes bronze a replayable record rather than just
        // a copy: it is the coordinate you go back to when something
        // downstream turns out to have been wrong.
        Dataset<Row> bronze = DeltaTables.read(spark, options.bronzePath());
        assertThat(bronze.filter("source_offset < 0").count()).isZero();
        assertThat(bronze.filter("source_partition < 0").count()).isZero();
        assertThat(bronze.select("source_offset").distinct().count())
                .as("one partition, so offsets are unique across the topic")
                .isEqualTo(produced.size());
    }

    @org.junit.jupiter.api.Order(3)
    @Test
    void eventsAreKeyedBySessionCode() {
        // Same key, same partition, so a session's events stay in order on the
        // topic. Nothing downstream depends on it, but it is what makes the raw
        // topic readable when somebody is debugging from offsets.
        Dataset<Row> bronze = DeltaTables.read(spark, options.bronzePath());
        assertThat(bronze.filter("source_key IS NULL").count()).isZero();
        assertThat(bronze.select("source_key").distinct().count()).isEqualTo(SESSIONS);
    }

    @org.junit.jupiter.api.Order(4)
    @Test
    void everyEventIsParsedIntoSilverExactlyOnce() {
        Dataset<Row> silver = DeltaTables.read(spark, options.silverPath());
        assertThat(silver.count()).isEqualTo(produced.size());
        assertThat(silver.select("event_id").distinct().count()).isEqualTo(produced.size());
        assertThat(DeltaTables.exists(spark, options.quarantinePath())).isFalse();
    }

    @org.junit.jupiter.api.Order(5)
    @Test
    void goldAggregatesReconcileWithWhatWasPublished() {
        Row totals = DeltaTables.read(spark, options.goldLifecyclePath())
                .agg(sum("sessions_created"), sum("event_count"))
                .first();
        assertThat(totals.getLong(0)).isEqualTo(SESSIONS);
        assertThat(totals.getLong(1)).isEqualTo(produced.size());

        assertThat(DeltaTables.read(spark, options.goldDurationPath()).count())
                .isEqualTo(SESSIONS);
    }

    @org.junit.jupiter.api.Order(6)
    @Test
    void aSecondRunConsumesNothingBecauseTheOffsetsAreCommitted() throws Exception {
        long before = DeltaTables.read(spark, options.bronzePath()).count();

        new BronzeIngestion(options, new KafkaEventSource(options)).runOnce(spark);

        assertThat(DeltaTables.read(spark, options.bronzePath()).count()).isEqualTo(before);
    }

    @org.junit.jupiter.api.Order(7)
    @Test
    void newEventsAfterACompletedRunArePickedUpFromTheCommittedOffset() throws Exception {
        long bronzeBefore = DeltaTables.read(spark, options.bronzePath()).count();
        long silverBefore = DeltaTables.read(spark, options.silverPath()).count();

        List<String> more = TestEvents.lines(20, Instant.parse("2026-08-27T11:00:00Z"), 78L);
        publish(more);

        new BronzeIngestion(options, new KafkaEventSource(options)).runOnce(spark);
        new SilverTransform(options).runOnce(spark);

        assertThat(DeltaTables.read(spark, options.bronzePath()).count())
                .isEqualTo(bronzeBefore + more.size());
        assertThat(DeltaTables.read(spark, options.silverPath()).count())
                .isEqualTo(silverBefore + more.size());
    }
}
