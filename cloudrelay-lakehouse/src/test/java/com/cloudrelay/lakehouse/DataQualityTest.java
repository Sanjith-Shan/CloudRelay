package com.cloudrelay.lakehouse;

import com.cloudrelay.lakehouse.ingest.BronzeIngestion;
import com.cloudrelay.lakehouse.ingest.FileEventSource;
import com.cloudrelay.lakehouse.transform.DataQuality;
import com.cloudrelay.lakehouse.transform.SilverTransform;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.array_contains;
import static org.apache.spark.sql.functions.col;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Bad events are routed, not dropped and not fatal.
 *
 * <p>Each case here is a different way a producer can go wrong, and the
 * assertion is always the same pair: the bad row lands in quarantine with a
 * reason attached, and the good rows around it are unaffected. The second half
 * is the one that matters operationally — a pipeline that stops on the first
 * malformed event is a pipeline that a single bad deploy takes offline.
 */
class DataQualityTest extends SparkTestBase {

    private static LakehouseOptions options;
    private static int goodEvents;

    private static final Instant START = Instant.parse("2026-08-27T09:00:00Z");

    @BeforeAll
    static void runPipeline() throws Exception {
        options = freshLakehouse("quality");

        List<String> lines = new ArrayList<>(
                TestEvents.lines(100, START, 5L));
        goodEvents = lines.size();

        lines.add("this is not json at all");
        lines.add("{\"eventId\":\"broken-json\",");
        lines.add(malformed("\"eventTime\":\"not-a-timestamp\"", "bad-timestamp"));
        lines.add(malformed("\"type\":\"PLAYER_TELEPORTED\"", "unknown-type"));
        lines.add(malformed("\"schemaVersion\":99", "future-schema"));
        lines.add(negativePlayerCount());
        lines.add(overCapacity());

        TestEvents.writeLanding(Path.of(options.landingPath()), lines, 6);

        new BronzeIngestion(options, new FileEventSource(options, 3)).runOnce(spark);
        new SilverTransform(options).runOnce(spark);
    }

    /** A well formed event with one field replaced by something invalid. */
    private static String malformed(String replacement, String id) {
        String valid = TestEvents.event("SESSION_CREATED", "BADCODE1", "cyberpunk-2077",
                "us-west-2", "WAITING", 1, 4, START, START, 0);
        String field = replacement.substring(0, replacement.indexOf(':'));
        int start = valid.indexOf(field);
        int end = valid.indexOf(',', start);
        return valid.substring(0, start) + replacement + valid.substring(end);
    }

    private static String negativePlayerCount() {
        return TestEvents.event("PLAYER_LEFT", "BADCODE2", "fortnite", "us-east-1",
                        "WAITING", 1, 4, START, START, 0)
                .replace("\"currentPlayerCount\":1", "\"currentPlayerCount\":-3");
    }

    private static String overCapacity() {
        return TestEvents.event("PLAYER_JOINED", "BADCODE3", "fortnite", "us-east-1",
                        "ACTIVE", 2, 4, START, START, 0)
                .replace("\"currentPlayerCount\":2", "\"currentPlayerCount\":9");
    }

    private Dataset<Row> quarantine() {
        return DeltaTables.read(spark, options.quarantinePath());
    }

    private long quarantinedFor(String rule) {
        return quarantine()
                .filter(array_contains(col(DataQuality.FAILURES_COLUMN), rule))
                .count();
    }

    /**
     * The rules broken by the single quarantined row whose raw payload contains
     * {@code needle}.
     *
     * <p>Asserting on per-rule totals instead would be misleading. A row whose
     * JSON does not parse has every field null, so it genuinely breaks every
     * rule at once and inflates each count by two. That behaviour is correct —
     * a row that is not an event is not a valid event either — but it means a
     * total is not evidence about any particular row. Pinning each case to the
     * row it is about keeps the assertion saying what it means.
     */
    private List<String> failuresFor(String needle) {
        List<Row> rows = quarantine()
                .filter(col("raw_value").contains(needle))
                .select(DataQuality.FAILURES_COLUMN)
                .collectAsList();
        assertThat(rows).as("exactly one row should contain %s", needle).hasSize(1);
        return rows.get(0).getList(0);
    }

    @Test
    void bronzeKeepsEverythingIncludingTheBadRows() {
        // Bronze is the record of what arrived. Filtering here would mean the
        // malformed events could never be replayed after the producer is fixed.
        assertThat(DeltaTables.read(spark, options.bronzePath()).count())
                .isEqualTo(goodEvents + 7);
    }

    @Test
    void goodEventsAreUnaffectedByBadNeighbours() {
        assertThat(DeltaTables.read(spark, options.silverPath()).count())
                .isEqualTo(goodEvents);
    }

    @Test
    void everyBadRowIsQuarantined() {
        assertThat(quarantine().count()).isEqualTo(7);
    }

    @Test
    void unparseableJsonIsCaughtByTheEnvelopeRule() {
        // Spark's PERMISSIVE parser turns these into all-null structs rather
        // than raising, so without this rule they would arrive in silver as
        // empty rows that pass every other check vacuously.
        assertThat(quarantinedFor("envelope_parses")).isEqualTo(2);
    }

    @Test
    void anUnparseableTimestampIsQuarantinedNotNulled() {
        assertThat(failuresFor("not-a-timestamp"))
                .containsExactly("event_time_parses");
    }

    @Test
    void anUnknownEventTypeIsQuarantined() {
        assertThat(failuresFor("PLAYER_TELEPORTED"))
                .containsExactly("event_type_known");
    }

    @Test
    void anEventFromANewerProducerIsQuarantined() {
        // Forward compatibility, in the safe direction: a version this job has
        // never seen is kept and replayable rather than parsed on a guess.
        assertThat(failuresFor("\"schemaVersion\":99"))
                .containsExactly("schema_version_supported");
    }

    @Test
    void aNegativePlayerCountIsQuarantined() {
        assertThat(failuresFor("\"currentPlayerCount\":-3"))
                .containsExactly("player_count_non_negative");
    }

    @Test
    void aPlayerCountOverCapacityIsQuarantined() {
        assertThat(failuresFor("\"currentPlayerCount\":9"))
                .containsExactly("player_count_within_capacity");
    }

    @Test
    void aRowThatIsNotEventJsonBreaksEveryRule() {
        // Worth pinning rather than leaving implicit: an all-null row fails
        // every expectation, and the quarantine record says so instead of
        // reporting only the first thing that went wrong.
        assertThat(quarantinedFor("envelope_parses")).isEqualTo(2);
        assertThat(quarantinedFor("event_time_parses"))
                .as("the two unparseable rows fail this too, on top of the bad timestamp")
                .isEqualTo(3);
    }

    @Test
    void quarantineKeepsTheRawPayloadForReplay() {
        Row row = quarantine().first();
        assertThat(row.<String>getAs("raw_value")).isNotBlank();
        assertThat(row.<java.sql.Timestamp>getAs("quarantined_at")).isNotNull();
        assertThat(row.<Integer>getAs("failure_count")).isPositive();
    }

    @Test
    void aRowCanFailSeveralRulesAtOnce() {
        // Unparseable JSON fails almost everything, and recording all of it is
        // the point: the first failure is rarely the whole story.
        assertThat(quarantine()
                .filter(col("failure_count").gt(1))
                .count())
                .isPositive();
    }
}
