package com.cloudrelay.lakehouse;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Every knob the medallion pipeline reads, resolved once at startup.
 *
 * <p>Jobs take this object rather than reaching for system properties, so a
 * test can point a whole pipeline at a temporary directory by constructing one
 * value. The table paths are derived from a single base path, which is what
 * makes "run the real pipeline against a scratch lakehouse" a one line change.
 */
public record LakehouseOptions(
        String basePath,
        String kafkaBootstrapServers,
        String kafkaTopic,
        String startingOffsets,
        TriggerMode triggerMode,
        Duration processingInterval,
        Duration goldWindow,
        int maxFilesPerTrigger,
        int shufflePartitions,
        String sparkMaster) {

    /**
     * How the streaming queries are driven.
     *
     * <p>{@code AVAILABLE_NOW} drains whatever is in the source and stops. It is
     * what the tests, the CI job and the batch style incremental runs use,
     * because it terminates on its own and is deterministic. {@code CONTINUOUS}
     * is the long lived daemon that a real deployment runs.
     */
    public enum TriggerMode {
        /**
         * Drain everything available, then stop. What the tests, CI and
         * scheduled catch-up runs use, because it terminates on its own.
         *
         * <p>One caveat worth knowing: against a Delta source this ignores
         * {@link #maxFilesPerTrigger()} and takes the whole backlog as a single
         * batch. That is usually what you want for a catch-up, and it is
         * exactly what you do not want when the batch structure is the thing
         * being studied — see {@link #RATE_LIMITED}.
         */
        AVAILABLE_NOW,

        /**
         * Drain everything, but in batches of {@link #maxFilesPerTrigger()},
         * then stop.
         *
         * <p>A backlog replayed as one enormous batch behaves nothing like the
         * live pipeline that produced it: different memory profile, different
         * file layout on the way out, and no way to reproduce the small-file
         * problem that a short trigger interval causes. This mode replays with
         * the batch structure a live job would have had.
         */
        RATE_LIMITED,

        /** The long lived daemon a deployment actually runs. */
        CONTINUOUS
    }

    /**
     * How many Delta files a single micro-batch may read from a table source.
     *
     * <p>This is the rate limiter. Left unbounded, a job restarting after a long
     * outage tries to process the entire backlog as one batch and dies on
     * memory; bounded, it catches up over many batches of predictable size.
     * Setting it low also makes the small-file problem reproducible on demand,
     * which is how the compaction benchmark builds a table worth compacting.
     */
    public static final String MAX_FILES_PER_TRIGGER = "maxFilesPerTrigger";

    public static final String DEFAULT_TOPIC = "cloudrelay.session-events";

    public static Builder builder() {
        return new Builder();
    }

    /** Sensible local defaults: a lakehouse under {@code ./data/lakehouse}. */
    public static LakehouseOptions localDefaults() {
        return builder().build();
    }

    // ---- Table locations -------------------------------------------------
    // One base path, everything hangs off it. Bronze/silver/gold are sibling
    // directories rather than nested, so a single table can be dropped and
    // rebuilt from the layer above it without touching the others.

    public String bronzePath()      { return table("bronze_session_events"); }
    public String silverPath()      { return table("silver_session_events"); }
    public String quarantinePath()  { return table("silver_quarantine"); }
    public String goldLifecyclePath()   { return table("gold_session_lifecycle_1m"); }
    public String goldConcurrencyPath() { return table("gold_region_concurrency_1m"); }
    public String goldDurationPath()    { return table("gold_session_duration"); }
    public String goldFunnelPath()      { return table("gold_matchmaking_funnel"); }

    public String checkpointPath(String queryName) {
        return path("_checkpoints", queryName);
    }

    /** Where the file based event source looks, when Kafka is not in play. */
    public String landingPath() { return path("landing"); }

    private String table(String name) { return path("tables", name); }

    private String path(String... parts) {
        Path p = Paths.get(basePath);
        for (String part : parts) {
            p = p.resolve(part);
        }
        return p.toAbsolutePath().toString();
    }

    /** Spark configuration that every job in this module shares. */
    public Map<String, String> sparkConf() {
        Map<String, String> conf = new HashMap<>();
        conf.put("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension");
        conf.put("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog");
        // A laptop scale pipeline. The default of 200 shuffle partitions turns
        // every aggregation into 200 near empty files, which is exactly the
        // small file problem this project sets out to measure deliberately
        // rather than stumble into.
        conf.put("spark.sql.shuffle.partitions", Integer.toString(shufflePartitions));
        conf.put("spark.databricks.delta.retentionDurationCheck.enabled", "false");
        conf.put("spark.sql.session.timeZone", "UTC");
        return conf;
    }

    public static final class Builder {
        private String basePath = "data/lakehouse";
        private String kafkaBootstrapServers = "localhost:9092";
        private String kafkaTopic = DEFAULT_TOPIC;
        private String startingOffsets = "earliest";
        private TriggerMode triggerMode = TriggerMode.AVAILABLE_NOW;
        private Duration processingInterval = Duration.ofSeconds(10);
        private Duration goldWindow = Duration.ofMinutes(1);
        private int maxFilesPerTrigger = 200;
        private int shufflePartitions = 8;
        private String sparkMaster = "local[*]";

        public Builder basePath(String v)              { this.basePath = v; return this; }
        public Builder kafkaBootstrapServers(String v) { this.kafkaBootstrapServers = v; return this; }
        public Builder kafkaTopic(String v)            { this.kafkaTopic = v; return this; }
        public Builder startingOffsets(String v)       { this.startingOffsets = v; return this; }
        public Builder triggerMode(TriggerMode v)      { this.triggerMode = v; return this; }
        public Builder processingInterval(Duration v)  { this.processingInterval = v; return this; }
        public Builder goldWindow(Duration v)          { this.goldWindow = v; return this; }
        public Builder maxFilesPerTrigger(int v)       { this.maxFilesPerTrigger = v; return this; }
        public Builder shufflePartitions(int v)        { this.shufflePartitions = v; return this; }
        public Builder sparkMaster(String v)           { this.sparkMaster = v; return this; }

        public LakehouseOptions build() {
            return new LakehouseOptions(basePath, kafkaBootstrapServers, kafkaTopic,
                    startingOffsets, triggerMode, processingInterval,
                    goldWindow, maxFilesPerTrigger, shufflePartitions, sparkMaster);
        }
    }
}
