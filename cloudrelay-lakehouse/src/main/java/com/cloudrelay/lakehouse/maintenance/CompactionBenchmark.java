package com.cloudrelay.lakehouse.maintenance;

import com.cloudrelay.lakehouse.LakehouseOptions;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Measures what OPTIMIZE actually buys, on this table, on this machine.
 *
 * <h2>How the timings are taken, and what they are not</h2>
 *
 * <p>Each query runs {@code warmups} times to get past JIT and Delta log
 * caching, then {@code iterations} times for the record, and the reported
 * number is the <em>median</em>. The median rather than the mean because a
 * single GC pause or a background process on a laptop moves a mean and does not
 * move a median. Spark's cache is cleared between phases so the second phase is
 * not reading the first phase's results.
 *
 * <p>What cannot be cleared is the operating system page cache. After the first
 * phase the file contents are warm in RAM, which flatters the second phase.
 * That biases the result in favour of OPTIMIZE, so the honest reading of a
 * speedup here is "an upper bound on a warm cache", and the file count and byte
 * totals — which are exact, come from the transaction log, and are not affected
 * by any of this — are the more trustworthy half of the report. Both halves get
 * printed for that reason.
 */
public final class CompactionBenchmark {

    /** Timings for one query, before and after compaction. */
    public record QueryTiming(String name, double beforeMs, double afterMs, long rowsReturned) {
        public double speedup() {
            return afterMs == 0 ? 0 : beforeMs / afterMs;
        }
    }

    /** Everything the benchmark measured, in one value. */
    public record Report(
            String tablePath,
            List<String> zorderColumns,
            int iterations,
            TableStats before,
            TableStats after,
            List<QueryTiming> timings,
            Instant capturedAt,
            String environment) {

        public double fileReduction() {
            return before.numFiles() == 0 ? 0
                    : 1.0 - ((double) after.numFiles() / before.numFiles());
        }

        public String toMarkdown() {
            StringBuilder sb = new StringBuilder();
            sb.append("### Compaction of `").append(tablePath).append("`\n\n");
            sb.append("Captured ").append(capturedAt).append(" on ").append(environment)
                    .append(". Median of ").append(iterations).append(" runs.\n\n");
            sb.append("| Measure | Before OPTIMIZE | After OPTIMIZE | Change |\n");
            sb.append("|---|---|---|---|\n");
            sb.append(String.format("| Files in table | %d | %d | %.1f%% fewer |%n",
                    before.numFiles(), after.numFiles(), fileReduction() * 100));
            sb.append(String.format("| Table size | %s | %s | %s |%n",
                    before.humanSize(), after.humanSize(),
                    TableStats.humanBytes(after.sizeInBytes() - before.sizeInBytes())));
            sb.append(String.format("| Average file | %s | %s | %.1fx larger |%n",
                    before.humanAvgFile(), after.humanAvgFile(),
                    before.avgFileBytes() == 0 ? 0
                            : (double) after.avgFileBytes() / before.avgFileBytes()));
            sb.append('\n');
            sb.append("| Query | Before | After | Speedup | Rows |\n");
            sb.append("|---|---|---|---|---|\n");
            for (QueryTiming t : timings) {
                sb.append(String.format("| `%s` | %.1f ms | %.1f ms | %.2fx | %d |%n",
                        t.name(), t.beforeMs(), t.afterMs(), t.speedup(), t.rowsReturned()));
            }
            sb.append("\nZ-ordered by: ")
                    .append(zorderColumns.isEmpty() ? "none (plain compaction)"
                            : String.join(", ", zorderColumns))
                    .append('\n');
            return sb.toString();
        }

        /** Machine readable, so the numbers can be regenerated and diffed. */
        public String toJson() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("table", tablePath);
            m.put("captured_at", capturedAt.toString());
            m.put("environment", environment);
            m.put("iterations", iterations);
            m.put("zorder_columns", zorderColumns);
            m.put("files_before", before.numFiles());
            m.put("files_after", after.numFiles());
            m.put("bytes_before", before.sizeInBytes());
            m.put("bytes_after", after.sizeInBytes());
            m.put("avg_file_bytes_before", before.avgFileBytes());
            m.put("avg_file_bytes_after", after.avgFileBytes());
            List<Map<String, Object>> queries = new ArrayList<>();
            for (QueryTiming t : timings) {
                Map<String, Object> q = new LinkedHashMap<>();
                q.put("name", t.name());
                q.put("before_ms", round(t.beforeMs()));
                q.put("after_ms", round(t.afterMs()));
                q.put("speedup", round(t.speedup()));
                q.put("rows", t.rowsReturned());
                queries.add(q);
            }
            m.put("queries", queries);
            return Json.object(m);
        }

        private static double round(double v) {
            return Math.round(v * 1000.0) / 1000.0;
        }
    }

    private final SparkSession spark;
    private final int warmups;
    private final int iterations;

    public CompactionBenchmark(SparkSession spark, int warmups, int iterations) {
        this.spark = spark;
        this.warmups = warmups;
        this.iterations = iterations;
    }

    /** The queries run against silver. Each one is chosen to stress a different cost. */
    public static List<BenchmarkQuery> silverQueries(String sampleSessionCode) {
        return List.of(
                // Dominated by file opens: the answer is in the metadata, so
                // whatever this costs is the price of touching every file.
                new BenchmarkQuery("count_all",
                        "SELECT count(*) FROM {table}"),
                // The case Z-order is for. One session's rows are scattered
                // across every file when rows land in arrival order; clustered
                // together, most files can be skipped on their min/max stats.
                new BenchmarkQuery("point_lookup_session",
                        "SELECT count(*) FROM {table} WHERE session_code = '"
                                + sampleSessionCode + "'"),
                // A realistic dashboard query: filter to one game and region,
                // then aggregate.
                new BenchmarkQuery("game_region_rollup",
                        "SELECT game_id, region, event_type, count(*) AS n FROM {table} "
                                + "WHERE game_id = 'cyberpunk-2077' AND region = 'us-west-2' "
                                + "GROUP BY game_id, region, event_type"),
                // Scan bound rather than metadata bound, as a control: if this
                // one barely moves while the others do, the gain really did
                // come from file pruning.
                new BenchmarkQuery("wide_aggregate",
                        "SELECT event_type, count(*) AS n, avg(player_count) AS avg_players "
                                + "FROM {table} GROUP BY event_type"));
    }

    public Report run(String tablePath, List<BenchmarkQuery> queries, List<String> zorderColumns) {
        TableStats before = TableStats.of(spark, tablePath);
        Map<String, Double> beforeMs = new LinkedHashMap<>();
        Map<String, Long> rows = new LinkedHashMap<>();
        for (BenchmarkQuery q : queries) {
            Timed t = time(q.resolve(tablePath));
            beforeMs.put(q.name(), t.medianMs());
            rows.put(q.name(), t.rows());
        }

        CompactionJob.optimizeZOrdered(spark, tablePath, zorderColumns);

        TableStats after = TableStats.of(spark, tablePath);
        List<CompactionBenchmark.QueryTiming> timings = new ArrayList<>();
        for (BenchmarkQuery q : queries) {
            Timed t = time(q.resolve(tablePath));
            timings.add(new QueryTiming(q.name(), beforeMs.get(q.name()), t.medianMs(), t.rows()));
        }

        return new Report(tablePath, zorderColumns, iterations, before, after, timings,
                Instant.now(), environmentLabel());
    }

    private record Timed(double medianMs, long rows) {
    }

    private Timed time(String sql) {
        long rows = 0;
        for (int i = 0; i < warmups; i++) {
            rows = spark.sql(sql).collectAsList().size();
        }
        double[] samples = new double[iterations];
        for (int i = 0; i < iterations; i++) {
            // Clearing between iterations keeps each one a cold Spark cache.
            // The OS page cache stays warm; see the class comment.
            spark.catalog().clearCache();
            long start = System.nanoTime();
            rows = spark.sql(sql).collectAsList().size();
            samples[i] = (System.nanoTime() - start) / 1_000_000.0;
        }
        Arrays.sort(samples);
        double median = iterations % 2 == 1
                ? samples[iterations / 2]
                : (samples[iterations / 2 - 1] + samples[iterations / 2]) / 2.0;
        return new Timed(median, rows);
    }

    /**
     * Recorded next to every number, because a benchmark without its machine is
     * not a result, it is a rumour.
     */
    public static String environmentLabel() {
        return System.getProperty("os.name") + " " + System.getProperty("os.arch")
                + ", " + Runtime.getRuntime().availableProcessors() + " cores, Java "
                + System.getProperty("java.version")
                + ", Spark " + org.apache.spark.package$.MODULE$.SPARK_VERSION()
                + ", local mode";
    }

    public static void writeReport(Report report, LakehouseOptions options) {
        Path dir = Path.of(options.basePath(), "_benchmarks");
        try {
            Files.createDirectories(dir);
            Files.writeString(dir.resolve("compaction.md"), report.toMarkdown(),
                    StandardCharsets.UTF_8);
            Files.writeString(dir.resolve("compaction.json"), report.toJson(),
                    StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
