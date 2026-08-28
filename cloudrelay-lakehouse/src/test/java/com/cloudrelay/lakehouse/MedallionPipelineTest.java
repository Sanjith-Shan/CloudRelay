package com.cloudrelay.lakehouse;

import com.cloudrelay.lakehouse.aggregate.GoldAggregates;
import com.cloudrelay.lakehouse.ingest.BronzeIngestion;
import com.cloudrelay.lakehouse.ingest.FileEventSource;
import com.cloudrelay.lakehouse.transform.SilverTransform;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Bronze to silver to gold, over a generated stream of session lifecycles.
 *
 * <p>Every assertion here is a count that has to reconcile against something
 * known about the input, not against the pipeline's own previous output. The
 * generator knows how many sessions it made and how many of them started, so
 * the gold funnel can be checked against the truth rather than against itself.
 */
class MedallionPipelineTest extends SparkTestBase {

    private static final int SESSIONS = 300;
    private static LakehouseOptions options;
    private static List<TestEvents.GeneratedSession> generated;
    private static int totalEvents;

    @BeforeAll
    static void runPipeline() throws Exception {
        options = freshLakehouse("medallion");
        generated = TestEvents.sessions(SESSIONS, Instant.parse("2026-08-27T09:00:00Z"), 42L);
        List<String> lines = TestEvents.flatten(generated);
        totalEvents = lines.size();

        TestEvents.writeLanding(Path.of(options.landingPath()), lines, 12);

        new BronzeIngestion(options, new FileEventSource(options, 4)).runOnce(spark);
        new SilverTransform(options).runOnce(spark);
        GoldAggregates gold = new GoldAggregates(options);
        gold.runOnce(spark);
    }

    private Dataset<Row> table(String path) {
        return DeltaTables.read(spark, path);
    }

    @Test
    void bronzeHoldsEveryEventExactlyOnce() {
        assertThat(table(options.bronzePath()).count()).isEqualTo(totalEvents);
    }

    @Test
    void bronzeKeepsTheRawPayloadUnparsed() {
        Row first = table(options.bronzePath()).first();
        assertThat(first.<String>getAs("raw_value")).startsWith("{").contains("\"eventId\"");
        assertThat(first.<java.sql.Timestamp>getAs("ingest_time")).isNotNull();
        assertThat(first.<java.sql.Date>getAs("ingest_date")).isNotNull();
    }

    @Test
    void bronzeIsPartitionedByIngestDate() {
        List<String> partitionColumns = spark
                .sql("DESCRIBE DETAIL delta.`" + options.bronzePath() + "`")
                .select("partitionColumns")
                .first()
                .getList(0);

        assertThat(partitionColumns).containsExactly("ingest_date");
    }

    @Test
    void everyValidEventReachesSilver() {
        assertThat(table(options.silverPath()).count()).isEqualTo(totalEvents);
    }

    @Test
    void nothingWasQuarantinedFromCleanInput() {
        // The quarantine table is only created when something fails, so its
        // absence is the pass condition for well formed input.
        assertThat(DeltaTables.exists(spark, options.quarantinePath()))
                .as("clean input should not produce a quarantine table")
                .isFalse();
    }

    @Test
    void silverIsTypedNotStrings() {
        Dataset<Row> silver = table(options.silverPath());
        assertThat(silver.schema().apply("event_time").dataType().typeName())
                .isEqualTo("timestamp");
        assertThat(silver.schema().apply("player_count").dataType().typeName())
                .isEqualTo("integer");
        assertThat(silver.schema().apply("is_full").dataType().typeName())
                .isEqualTo("boolean");
    }

    @Test
    void silverDropsTheRawPayloadItNoLongerNeeds() {
        // Bronze is the replayable copy. Carrying the raw JSON through silver
        // as well would roughly double the table for no reader.
        assertThat(table(options.silverPath()).schema().fieldNames())
                .doesNotContain("raw_value");
    }

    @Test
    void everyEventIdInSilverIsUnique() {
        Dataset<Row> silver = table(options.silverPath());
        assertThat(silver.select("event_id").distinct().count())
                .isEqualTo(silver.count());
    }

    @Test
    void goldLifecycleCountsEverySessionCreation() {
        Row totals = table(options.goldLifecyclePath())
                .agg(sum("sessions_created").as("created"),
                        sum("sessions_terminated").as("terminated"),
                        sum("event_count").as("events"))
                .first();

        assertThat(totals.getLong(0)).isEqualTo(SESSIONS);
        assertThat(totals.getLong(1)).isEqualTo(SESSIONS);
        assertThat(totals.getLong(2)).isEqualTo(totalEvents);
    }

    @Test
    void goldLifecycleIsBucketedByMinute() {
        Dataset<Row> lifecycle = table(options.goldLifecyclePath());
        assertThat(lifecycle.count()).isGreaterThan(1);
        Row window = lifecycle.select("window_start", "window_end").first();
        long seconds = (window.getTimestamp(1).getTime() - window.getTimestamp(0).getTime()) / 1000;
        assertThat(seconds).isEqualTo(60);
    }

    @Test
    void goldLifecycleHasOneRowPerWindowGameAndRegion() {
        // The MERGE is keyed on exactly this triple. A duplicate here would
        // mean the recompute inserted where it should have updated.
        Dataset<Row> lifecycle = table(options.goldLifecyclePath());
        assertThat(lifecycle.select("window_start", "game_id", "region").distinct().count())
                .isEqualTo(lifecycle.count());
    }

    @Test
    void goldConcurrencyCoversEveryRegionSeen() {
        assertThat(table(options.goldConcurrencyPath()).select("region").distinct().count())
                .isEqualTo(TestEvents.REGIONS.size());
    }

    @Test
    void goldDurationHasOneRowPerSession() {
        Dataset<Row> durations = table(options.goldDurationPath());
        assertThat(durations.count()).isEqualTo(SESSIONS);
        assertThat(durations.filter(col("completed").equalTo(true)).count())
                .as("every generated session terminates")
                .isEqualTo(SESSIONS);
    }

    @Test
    void goldDurationReportsPlausibleLengths() {
        Row stats = spark.sql("SELECT min(duration_seconds), max(duration_seconds) "
                + "FROM delta.`" + options.goldDurationPath() + "`").first();
        assertThat(stats.getLong(0)).isPositive();
        // Generated sessions run for at most a join sequence plus a 30 minute
        // game. Anything beyond that means durations are being derived from
        // pipeline timing rather than from the events.
        assertThat(stats.getLong(1)).isLessThan(3600);
    }

    @Test
    void goldFunnelMatchesTheSessionsThatActuallyStarted() {
        long expectedStarted = generated.stream().filter(TestEvents.GeneratedSession::started)
                .count();

        Row totals = table(options.goldFunnelPath())
                .agg(sum("sessions_observed"), sum("sessions_started"))
                .first();

        assertThat(totals.getLong(0)).isEqualTo(SESSIONS);
        assertThat(totals.getLong(1)).isEqualTo(expectedStarted);
    }

    @Test
    void goldFunnelStartRateIsAProportion() {
        List<Row> rows = table(options.goldFunnelPath()).select("start_rate").collectAsList();
        assertThat(rows).isNotEmpty();
        assertThat(rows).allSatisfy(row ->
                assertThat(row.getDouble(0)).isBetween(0.0, 1.0));
    }

    @Test
    void rerunningTheWholePipelineChangesNothing() throws Exception {
        long bronzeBefore = table(options.bronzePath()).count();
        long silverBefore = table(options.silverPath()).count();
        long lifecycleBefore = table(options.goldLifecyclePath()).count();

        // Every source is already fully consumed and every checkpoint records
        // it, so a second run is a no-op. This is the property that makes the
        // pipeline safe to put on a schedule.
        new BronzeIngestion(options, new FileEventSource(options, 4)).runOnce(spark);
        new SilverTransform(options).runOnce(spark);
        new GoldAggregates(options).runBatchAggregates(spark);

        assertThat(table(options.bronzePath()).count()).isEqualTo(bronzeBefore);
        assertThat(table(options.silverPath()).count()).isEqualTo(silverBefore);
        assertThat(table(options.goldLifecyclePath()).count()).isEqualTo(lifecycleBefore);
        assertThat(table(options.goldDurationPath()).count()).isEqualTo(SESSIONS);
    }
}
