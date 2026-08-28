package com.cloudrelay.lakehouse;

import com.cloudrelay.lakehouse.aggregate.GoldAggregates;
import com.cloudrelay.lakehouse.ingest.BronzeIngestion;
import com.cloudrelay.lakehouse.ingest.EventSource;
import com.cloudrelay.lakehouse.ingest.FileEventSource;
import com.cloudrelay.lakehouse.ingest.KafkaEventSource;
import com.cloudrelay.lakehouse.maintenance.BenchmarkQuery;
import com.cloudrelay.lakehouse.maintenance.CompactionBenchmark;
import com.cloudrelay.lakehouse.maintenance.CompactionJob;
import com.cloudrelay.lakehouse.query.AnalyticsQueries;
import com.cloudrelay.lakehouse.query.TimeTravel;
import com.cloudrelay.lakehouse.transform.SilverTransform;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Entry point for the lakehouse. One class, several verbs.
 *
 * <pre>
 *   run         bronze then silver then gold, end to end
 *   bronze      ingest only
 *   silver      parse, validate and deduplicate only
 *   gold        aggregate only
 *   compact     OPTIMIZE with before and after measurements
 *   query       run the analytics pack against gold
 *   history     Delta version history with row counts
 * </pre>
 *
 * <p>Flags: {@code --base-path}, {@code --source kafka|file},
 * {@code --kafka host:port}, {@code --topic name}, {@code --continuous},
 * {@code --max-files-per-trigger N}, {@code --rate-limited},
 * {@code --iterations N}.
 *
 * <p>The default trigger drains what is available and stops, which is what makes
 * a run scriptable and a CI job possible. {@code --continuous} is the long
 * lived daemon a deployment would actually run.
 */
public final class MedallionPipeline {

    private MedallionPipeline() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("usage: MedallionPipeline <run|bronze|silver|gold|compact|"
                    + "query|history> [flags]");
            return;
        }
        String command = args[0];
        Map<String, String> flags = parseFlags(args);
        LakehouseOptions options = optionsFrom(flags);

        SparkSession spark = SparkSessions.create("cloudrelay-" + command, options);
        try {
            switch (command) {
                case "run" -> runAll(spark, options, flags);
                case "bronze" -> new BronzeIngestion(options, sourceFrom(options, flags))
                        .runOnce(spark);
                case "silver" -> new SilverTransform(options).runOnce(spark);
                case "gold" -> new GoldAggregates(options).runOnce(spark);
                case "compact" -> compact(spark, options, flags);
                case "query" -> new AnalyticsQueries(options).runAll(spark)
                        .forEach(System.out::println);
                case "history" -> history(spark, options);
                default -> System.out.println("unknown command: " + command);
            }
        } finally {
            spark.stop();
        }
    }

    /**
     * The layers run in order rather than concurrently.
     *
     * <p>Under {@code AVAILABLE_NOW} each stage drains its source and stops, so
     * running them in sequence guarantees gold sees everything silver produced
     * from everything bronze ingested — one pass, fully settled, which is what
     * a test or a scripted demo needs to assert on. A deployment starts the
     * three queries together and lets them run; that is what
     * {@code --continuous} does, and there the stages are genuinely concurrent
     * with each layer trailing the one above it by a trigger interval.
     */
    private static void runAll(SparkSession spark, LakehouseOptions options,
                               Map<String, String> flags) throws Exception {
        EventSource source = sourceFrom(options, flags);
        System.out.println("[cloudrelay] bronze <- " + source.describe());
        new BronzeIngestion(options, source).runOnce(spark);

        System.out.println("[cloudrelay] silver <- bronze");
        new SilverTransform(options).runOnce(spark);

        System.out.println("[cloudrelay] gold <- silver");
        new GoldAggregates(options).runOnce(spark);

        System.out.println("[cloudrelay] done");
        summarise(spark, options);
    }

    private static void summarise(SparkSession spark, LakehouseOptions options) {
        for (String[] table : new String[][]{
                {"bronze", options.bronzePath()},
                {"silver", options.silverPath()},
                {"quarantine", options.quarantinePath()},
                {"gold lifecycle", options.goldLifecyclePath()},
                {"gold concurrency", options.goldConcurrencyPath()},
                {"gold duration", options.goldDurationPath()},
                {"gold funnel", options.goldFunnelPath()}}) {
            if (DeltaTables.exists(spark, table[1])) {
                System.out.printf("  %-18s %,d rows%n", table[0],
                        DeltaTables.read(spark, table[1]).count());
            }
        }
    }

    private static void compact(SparkSession spark, LakehouseOptions options,
                                Map<String, String> flags) {
        String path = flags.getOrDefault("table", options.silverPath());
        int iterations = Integer.parseInt(flags.getOrDefault("iterations", "7"));
        int warmups = Integer.parseInt(flags.getOrDefault("warmups", "2"));

        if (!DeltaTables.exists(spark, path)) {
            System.out.println("[cloudrelay] nothing to compact: " + path + " does not exist yet");
            return;
        }

        // Pick a session code that is actually in the table, so the point
        // lookup measures a hit rather than the cost of proving a miss.
        Dataset<Row> silver = DeltaTables.read(spark, path);
        List<Row> sample = silver.select("session_code").limit(1).collectAsList();
        if (sample.isEmpty()) {
            System.out.println("[cloudrelay] nothing to compact: " + path + " is empty");
            return;
        }
        String sampleCode = sample.get(0).getString(0);

        List<BenchmarkQuery> queries = CompactionBenchmark.silverQueries(sampleCode);
        CompactionBenchmark.Report report =
                new CompactionBenchmark(spark, warmups, iterations)
                        .run(path, queries, List.of("game_id", "session_code"));

        System.out.println(report.toMarkdown());
        CompactionBenchmark.writeReport(report, options);
        System.out.println("[cloudrelay] report written to "
                + options.basePath() + "/_benchmarks/");

        if (flags.containsKey("vacuum")) {
            CompactionJob.vacuum(spark, path, 0);
            System.out.println("[cloudrelay] vacuumed; superseded files removed");
        }
    }

    private static void history(SparkSession spark, LakehouseOptions options) {
        for (String path : List.of(options.bronzePath(), options.silverPath())) {
            if (DeltaTables.exists(spark, path)) {
                System.out.println(TimeTravel.render(path,
                        TimeTravel.history(spark, path, 20)));
            }
        }
    }

    // ---- Flags ------------------------------------------------------------

    private static EventSource sourceFrom(LakehouseOptions options, Map<String, String> flags) {
        String kind = flags.getOrDefault("source", "kafka");
        return "file".equals(kind)
                ? new FileEventSource(options, options.maxFilesPerTrigger())
                : new KafkaEventSource(options);
    }

    private static LakehouseOptions optionsFrom(Map<String, String> flags) {
        LakehouseOptions.Builder builder = LakehouseOptions.builder()
                .basePath(flags.getOrDefault("base-path", "data/lakehouse"))
                .kafkaBootstrapServers(flags.getOrDefault("kafka", "localhost:9092"))
                .kafkaTopic(flags.getOrDefault("topic", LakehouseOptions.DEFAULT_TOPIC))
                .startingOffsets(flags.getOrDefault("starting-offsets", "earliest"))
                .shufflePartitions(Integer.parseInt(flags.getOrDefault("shuffle-partitions", "8")))
                // One flag, one meaning: how many files any source hands a
                // single micro-batch. Setting it to 1 is how the small-file
                // problem is reproduced on demand for the compaction benchmark.
                .maxFilesPerTrigger(
                        Integer.parseInt(flags.getOrDefault("max-files-per-trigger", "200")));
        if (flags.containsKey("rate-limited")) {
            builder.triggerMode(LakehouseOptions.TriggerMode.RATE_LIMITED);
        }
        if (flags.containsKey("continuous")) {
            builder.triggerMode(LakehouseOptions.TriggerMode.CONTINUOUS)
                    .processingInterval(Duration.ofSeconds(
                            Long.parseLong(flags.getOrDefault("interval-seconds", "10"))));
        }
        if (flags.containsKey("master")) {
            builder.sparkMaster(flags.get("master"));
        }
        return builder.build();
    }

    /** {@code --key value} and bare {@code --flag} forms, nothing cleverer. */
    static Map<String, String> parseFlags(String[] args) {
        Map<String, String> flags = new HashMap<>();
        for (int i = 1; i < args.length; i++) {
            if (!args[i].startsWith("--")) {
                continue;
            }
            String key = args[i].substring(2);
            boolean hasValue = i + 1 < args.length && !args[i + 1].startsWith("--");
            flags.put(key, hasValue ? args[++i] : "true");
        }
        return flags;
    }
}
