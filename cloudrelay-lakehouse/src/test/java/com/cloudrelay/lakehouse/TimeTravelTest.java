package com.cloudrelay.lakehouse;

import com.cloudrelay.lakehouse.ingest.BronzeIngestion;
import com.cloudrelay.lakehouse.ingest.FileEventSource;
import com.cloudrelay.lakehouse.query.TimeTravel;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reading a Delta table as it stood at an earlier commit.
 *
 * <p>The mechanism is worth stating precisely, because "time travel" makes it
 * sound like snapshots are being kept. They are not. A Delta table is an ordered
 * log of commits, each recording which files were added and which removed. The
 * current table is that log replayed to the end; version 3 is the same replay
 * stopped at commit 3. Nothing is copied, and the cost of keeping history is the
 * cost of not having deleted the old files yet.
 */
class TimeTravelTest extends SparkTestBase {

    private static LakehouseOptions options;
    private static long totalEvents;

    @BeforeAll
    static void ingestInSeveralCommits() throws Exception {
        options = LakehouseOptions.builder()
                .basePath(freshLakehouse("time-travel").basePath())
                .maxFilesPerTrigger(1)
                .shufflePartitions(2)
                .sparkMaster("local[2]")
                .build();

        List<String> lines =
                TestEvents.lines(120, Instant.parse("2026-08-27T09:00:00Z"), 23L);
        totalEvents = lines.size();
        TestEvents.writeLanding(Path.of(options.landingPath()), lines, 10);

        // One file per trigger, so each batch is its own commit and the table
        // ends up with a history worth travelling through.
        new BronzeIngestion(options, new FileEventSource(options, 1)).runOnce(spark);
    }

    @Test
    void theTableHasACommitPerMicroBatch() {
        assertThat(DeltaTables.latestVersion(spark, options.bronzePath()))
                .isGreaterThanOrEqualTo(5);
    }

    @Test
    void anEarlierVersionHoldsFewerRows() {
        long latest = DeltaTables.latestVersion(spark, options.bronzePath());

        long atFirstCommit = DeltaTables.readVersion(spark, options.bronzePath(), 0).count();
        long atLatest = DeltaTables.read(spark, options.bronzePath()).count();

        assertThat(atLatest).isEqualTo(totalEvents);
        assertThat(atFirstCommit)
                .as("version 0 is the first batch only")
                .isPositive()
                .isLessThan(atLatest);
        assertThat(DeltaTables.readVersion(spark, options.bronzePath(), latest).count())
                .isEqualTo(atLatest);
    }

    @Test
    void rowCountsGrowMonotonicallyThroughHistory() {
        List<TimeTravel.Version> history =
                TimeTravel.history(spark, options.bronzePath(), 50);

        assertThat(history).isNotEmpty();
        List<TimeTravel.Version> oldestFirst = history.stream()
                .sorted(java.util.Comparator.comparingLong(TimeTravel.Version::version))
                .toList();

        long previous = -1;
        for (TimeTravel.Version version : oldestFirst) {
            assertThat(version.rowCount())
                    .as("an append-only table never shrinks as it moves forward")
                    .isGreaterThan(previous);
            previous = version.rowCount();
        }
        assertThat(oldestFirst.get(oldestFirst.size() - 1).rowCount()).isEqualTo(totalEvents);
    }

    @Test
    void historyRecordsWhatEachCommitDid() {
        List<TimeTravel.Version> history =
                TimeTravel.history(spark, options.bronzePath(), 5);

        assertThat(history).allSatisfy(version -> {
            assertThat(version.operation()).isNotBlank();
            assertThat(version.timestamp()).isNotBlank();
        });
        assertThat(TimeTravel.render(options.bronzePath(), history))
                .contains("version").contains("rows visible");
    }
}
