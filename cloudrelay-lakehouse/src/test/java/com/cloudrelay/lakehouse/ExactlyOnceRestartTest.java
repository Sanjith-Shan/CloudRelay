package com.cloudrelay.lakehouse;

import com.cloudrelay.lakehouse.ingest.BronzeIngestion;
import com.cloudrelay.lakehouse.ingest.FileEventSource;
import com.cloudrelay.lakehouse.transform.SilverTransform;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the pipeline neither loses nor duplicates events when it is killed and
 * restarted.
 *
 * <p>This is the claim in the whole project that is hardest to make honestly, so
 * it gets tested rather than asserted in a README. It also has two halves that
 * are genuinely different problems, and a pipeline needs both:
 *
 * <ul>
 *   <li><b>The pipeline crashed.</b> Spark had read a batch and not yet
 *       committed its offsets. On restart it re-reads from the last committed
 *       offset and rewrites work it may already have done. Checkpointed offsets
 *       plus Delta's atomic commits are what make the rewrite invisible.</li>
 *   <li><b>The producer retried.</b> The service's Kafka send timed out and was
 *       retried, so two genuinely identical events exist upstream. No amount of
 *       checkpointing helps: both are real records that really arrived. The
 *       watermarked dedup on {@code event_id} is what absorbs these.</li>
 * </ul>
 *
 * <p>A pipeline that handles one and not the other is not exactly-once, and
 * which one is missing is usually invisible until it matters.
 */
class ExactlyOnceRestartTest extends SparkTestBase {

    private static final Pattern EVENT_ID =
            Pattern.compile("\"eventId\":\"([0-9a-f-]+)\"");

    private static Set<String> eventIdsIn(List<String> lines) {
        return lines.stream().map(line -> {
            Matcher m = EVENT_ID.matcher(line);
            return m.find() ? m.group(1) : null;
        }).filter(java.util.Objects::nonNull).collect(Collectors.toSet());
    }

    @Test
    void aBronzeCrashMidStreamLosesAndDuplicatesNothing() throws Exception {
        LakehouseOptions options = freshLakehouse("restart-bronze");
        List<String> lines =
                TestEvents.lines(200, Instant.parse("2026-08-27T09:00:00Z"), 7L);
        // One file per trigger over many files, so a batch boundary is a known
        // place and the kill lands somewhere in the middle rather than
        // wherever a timer happened to fire.
        TestEvents.writeLanding(Path.of(options.landingPath()), lines, 60);

        BronzeIngestion bronze =
                new BronzeIngestion(options, new FileEventSource(options, 1));

        StreamingQuery killed = bronze.startBounded(spark, 2);
        assertThat(killed.isActive()).isFalse();

        long afterCrash = DeltaTables.read(spark, options.bronzePath()).count();
        assertThat(afterCrash)
                .as("the kill should land mid-stream, not before or after it")
                .isPositive()
                .isLessThan(lines.size());

        // Restart from the same checkpoint and drain the rest.
        bronze.runOnce(spark);

        Dataset<Row> ingested = DeltaTables.read(spark, options.bronzePath());
        assertThat(ingested.count())
                .as("no event lost and none ingested twice")
                .isEqualTo(lines.size());
        assertThat(ingested.select("raw_value").distinct().count())
                .as("every raw payload appears exactly once")
                .isEqualTo(lines.size());
    }

    /**
     * The same duplicates, but split so that the copy lands in a different
     * micro-batch from the original.
     *
     * <p>This is the case a watermarked {@code dropDuplicatesWithinWatermark}
     * gets wrong once the gap grows beyond its delay, and the case that made
     * this pipeline move its deduplication into a Delta MERGE. The identity
     * check lives in the table, so how far apart the two copies arrive does not
     * matter.
     */
    @Test
    void duplicatesArrivingInSeparateBatchesAreStillDeduplicated() throws Exception {
        LakehouseOptions options = LakehouseOptions.builder()
                .basePath(freshLakehouse("dedup-across-batches").basePath())
                .maxFilesPerTrigger(1)
                .shufflePartitions(2)
                .sparkMaster("local[2]")
                .build();

        List<String> original =
                TestEvents.lines(120, Instant.parse("2026-08-27T09:00:00Z"), 29L);

        // Originals first, every copy afterwards: with one file per trigger the
        // second copy is many batches behind its twin.
        List<String> withRetries = new ArrayList<>(original);
        withRetries.addAll(original);
        TestEvents.writeLanding(Path.of(options.landingPath()), withRetries, 16);

        new BronzeIngestion(options, new FileEventSource(options, 1)).runOnce(spark);
        new SilverTransform(options).runOnce(spark);

        assertThat(DeltaTables.read(spark, options.bronzePath()).count())
                .isEqualTo(original.size() * 2L);
        assertThat(DeltaTables.read(spark, options.silverPath()).count())
                .as("no event lost to lateness and none counted twice")
                .isEqualTo(original.size());
    }

    @Test
    void aSilverCrashMidStreamLosesAndDuplicatesNothing() throws Exception {
        LakehouseOptions options = freshLakehouse("restart-silver");
        List<String> lines =
                TestEvents.lines(200, Instant.parse("2026-08-27T09:00:00Z"), 11L);
        TestEvents.writeLanding(Path.of(options.landingPath()), lines, 40);

        new BronzeIngestion(options, new FileEventSource(options, 8)).runOnce(spark);

        SilverTransform silver = new SilverTransform(options);

        // Start silver, let a couple of batches through, then kill it.
        StreamingQuery query = silver.start(spark);
        while (query.isActive() && query.recentProgress().length < 2) {
            Thread.sleep(50);
        }
        query.stop();

        silver.runOnce(spark);

        Dataset<Row> parsed = DeltaTables.read(spark, options.silverPath());
        assertThat(parsed.count()).isEqualTo(lines.size());
        assertThat(parsed.select("event_id").distinct().count())
                .as("no event id appears twice after the restart")
                .isEqualTo(lines.size());
        assertThat(parsed.select("event_id").collectAsList().stream()
                .map(r -> r.getString(0)).collect(Collectors.toSet()))
                .as("the surviving rows are exactly the events that were produced")
                .isEqualTo(eventIdsIn(lines));
    }

    @Test
    void producerRetriesAreDeduplicatedInSilver() throws Exception {
        LakehouseOptions options = freshLakehouse("dedup");

        List<String> original =
                TestEvents.lines(150, Instant.parse("2026-08-27T09:00:00Z"), 13L);

        // Every event immediately followed by an identical copy, exactly as a
        // producer retry after an ack timeout would leave the topic.
        List<String> withRetries = new ArrayList<>(original.size() * 2);
        for (String line : original) {
            withRetries.add(line);
            withRetries.add(line);
        }
        TestEvents.writeLanding(Path.of(options.landingPath()), withRetries, 10);

        new BronzeIngestion(options, new FileEventSource(options, 4)).runOnce(spark);
        new SilverTransform(options).runOnce(spark);

        assertThat(DeltaTables.read(spark, options.bronzePath()).count())
                .as("bronze keeps both copies; it records what arrived, not what is true")
                .isEqualTo(original.size() * 2L);

        Dataset<Row> silver = DeltaTables.read(spark, options.silverPath());
        assertThat(silver.count())
                .as("silver holds one row per distinct event id")
                .isEqualTo(original.size());
        assertThat(silver.select("event_id").collectAsList().stream()
                .map(r -> r.getString(0)).collect(Collectors.toSet()))
                .isEqualTo(eventIdsIn(original));
    }

    @Test
    void replayingAnAlreadyConsumedSourceAddsNothing() throws Exception {
        LakehouseOptions options = freshLakehouse("replay");
        List<String> lines =
                TestEvents.lines(120, Instant.parse("2026-08-27T09:00:00Z"), 17L);
        TestEvents.writeLanding(Path.of(options.landingPath()), lines, 8);

        BronzeIngestion bronze =
                new BronzeIngestion(options, new FileEventSource(options, 4));
        bronze.runOnce(spark);
        new SilverTransform(options).runOnce(spark);

        long bronzeRows = DeltaTables.read(spark, options.bronzePath()).count();
        long silverRows = DeltaTables.read(spark, options.silverPath()).count();

        // Running the job again is the normal case, not the exceptional one:
        // it is what a scheduler does every interval, and what an operator does
        // after fixing something unrelated. It has to be free.
        bronze.runOnce(spark);
        new SilverTransform(options).runOnce(spark);

        assertThat(DeltaTables.read(spark, options.bronzePath()).count())
                .isEqualTo(bronzeRows);
        assertThat(DeltaTables.read(spark, options.silverPath()).count())
                .isEqualTo(silverRows);
    }
}
