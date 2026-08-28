package com.cloudrelay.lakehouse;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

/**
 * One {@link SparkSession} for the whole test class.
 *
 * <p>Starting a local Spark costs several seconds, which is most of the runtime
 * of a small test if it is paid per method. The session is stateless as far as
 * these tests are concerned — each one gets its own lakehouse directory, so
 * sharing the session shares no data.
 */
public abstract class SparkTestBase {

    protected static SparkSession spark;

    @BeforeAll
    static void startSpark() {
        spark = SparkSessions.create("cloudrelay-tests",
                LakehouseOptions.builder()
                        // Two shuffle partitions, not the default 200. At test
                        // volumes the default produces 200 files of a few
                        // hundred bytes per aggregation and turns a fast test
                        // into a slow one.
                        .shufflePartitions(2)
                        .sparkMaster("local[2]")
                        .build());
    }

    @AfterAll
    static void stopSpark() {
        if (spark != null) {
            spark.stop();
            spark = null;
        }
    }

    /** A fresh lakehouse directory, removed when the JVM exits. */
    protected static LakehouseOptions freshLakehouse(String name) throws IOException {
        Path dir = Files.createTempDirectory("cloudrelay-" + name);
        dir.toFile().deleteOnExit();
        return LakehouseOptions.builder()
                .basePath(dir.toAbsolutePath().toString())
                .shufflePartitions(2)
                .sparkMaster("local[2]")
                .build();
    }

    protected static void deleteRecursively(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (var paths = Files.walk(root)) {
            paths.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        }
    }
}
