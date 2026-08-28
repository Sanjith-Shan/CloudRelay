package com.cloudrelay.lakehouse;

import com.cloudrelay.lakehouse.ingest.BronzeIngestion;
import com.cloudrelay.lakehouse.ingest.FileEventSource;
import com.cloudrelay.lakehouse.maintenance.BenchmarkQuery;
import com.cloudrelay.lakehouse.maintenance.CompactionBenchmark;
import com.cloudrelay.lakehouse.maintenance.CompactionJob;
import com.cloudrelay.lakehouse.maintenance.TableStats;
import com.cloudrelay.lakehouse.transform.SilverTransform;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The small file problem, caused deliberately and then measured.
 *
 * <p>Streaming ingestion produces it by construction: a micro-batch that carried
 * forty rows still writes a file, and a table accumulates one per batch per
 * partition forever. This test reproduces that by rate limiting both stages to
 * one file per trigger, which is the same thing a low-latency trigger does on a
 * quiet topic — many batches, each of them small.
 *
 * <p>What is asserted and what is only reported are different on purpose. File
 * counts and byte totals come from the Delta transaction log, are exact, and are
 * asserted. Query timings on a laptop share a machine with everything else the
 * laptop is doing and are reported without an assertion, because a threshold on
 * them would be a flaky test rather than a real guarantee.
 */
@org.junit.jupiter.api.TestMethodOrder(
        org.junit.jupiter.api.MethodOrderer.OrderAnnotation.class)
class CompactionBenchmarkTest extends SparkTestBase {

    private static LakehouseOptions options;
    private static String sampleSessionCode;

    @BeforeAll
    static void buildAFragmentedTable() throws Exception {
        options = LakehouseOptions.builder()
                .basePath(freshLakehouse("compaction").basePath())
                // One file per micro-batch on both hops. This is the knob that
                // turns a tidy table into a fragmented one on demand.
                .maxFilesPerTrigger(1)
                .shufflePartitions(2)
                .sparkMaster("local[2]")
                .build();

        List<String> lines =
                TestEvents.lines(2_000, Instant.parse("2026-08-27T09:00:00Z"), 99L);
        TestEvents.writeLanding(Path.of(options.landingPath()), lines, 50);

        new BronzeIngestion(options, new FileEventSource(options, 1)).runOnce(spark);
        new SilverTransform(options).runOnce(spark);

        sampleSessionCode = DeltaTables.read(spark, options.silverPath())
                .select("session_code").first().getString(0);
    }

    @org.junit.jupiter.api.Order(1)
    @Test
    void streamingIngestionFragmentsTheTable() {
        TableStats stats = TableStats.of(spark, options.silverPath());

        assertThat(stats.numFiles())
                .as("many micro-batches should leave many files behind")
                .isGreaterThan(20);
        assertThat(stats.avgFileBytes())
                .as("and each of them should be small")
                .isLessThan(256 * 1024);
    }

    @org.junit.jupiter.api.Order(2)
    @Test
    void optimizeCompactsTheTableAndTheQueriesGetFaster() throws IOException {
        CompactionBenchmark benchmark = new CompactionBenchmark(spark, 1, 5);
        List<BenchmarkQuery> queries = CompactionBenchmark.silverQueries(sampleSessionCode);

        CompactionBenchmark.Report report = benchmark.run(
                options.silverPath(), queries, List.of("game_id", "session_code"));

        System.out.println(report.toMarkdown());
        Path out = Path.of("target", "benchmarks");
        Files.createDirectories(out);
        Files.writeString(out.resolve("compaction.md"), report.toMarkdown());
        Files.writeString(out.resolve("compaction.json"), report.toJson());

        assertThat(report.after().numFiles())
                .as("OPTIMIZE must leave fewer files than it found")
                .isLessThan(report.before().numFiles());
        assertThat(report.after().avgFileBytes())
                .as("and the files it leaves must be bigger")
                .isGreaterThan(report.before().avgFileBytes());
        assertThat(report.fileReduction()).isGreaterThan(0.5);

        // Compaction rewrites rows, it does not remove them. Getting this wrong
        // is the failure mode that matters, and a file count alone would not
        // catch it.
        assertThat(report.timings()).allSatisfy(timing ->
                assertThat(timing.rowsReturned()).isPositive());
        assertThat(DeltaTables.read(spark, options.silverPath()).count())
                .isEqualTo(DeltaTables.read(spark, options.bronzePath()).count());
    }

    @org.junit.jupiter.api.Order(3)
    @Test
    void vacuumReclaimsTheSupersededFilesFromDisk() throws Exception {
        // Compacts here rather than relying on another test having run: JUnit
        // does not promise an order, and a test that only passes after a
        // sibling is a test that fails for reasons that have nothing to do with
        // what it checks.
        CompactionJob.optimize(spark, options.silverPath());

        // OPTIMIZE only rewrites the log. The originals stay on disk so that
        // readers mid-query and time travel to earlier versions keep working,
        // and only VACUUM actually frees the space.
        long onDiskBefore = parquetBytesOnDisk(Path.of(options.silverPath()));
        long inTableBefore = TableStats.of(spark, options.silverPath()).sizeInBytes();

        assertThat(onDiskBefore)
                .as("after an OPTIMIZE the directory holds more than the table does")
                .isGreaterThan(inTableBefore);

        CompactionJob.vacuum(spark, options.silverPath(), 0);

        long onDiskAfter = parquetBytesOnDisk(Path.of(options.silverPath()));
        assertThat(onDiskAfter).isLessThan(onDiskBefore);
        assertThat(DeltaTables.read(spark, options.silverPath()).count()).isPositive();
    }

    private static long parquetBytesOnDisk(Path tableRoot) throws IOException {
        try (var paths = Files.walk(tableRoot)) {
            return paths.filter(p -> p.toString().endsWith(".parquet"))
                    .mapToLong(p -> p.toFile().length())
                    .sum();
        }
    }
}
