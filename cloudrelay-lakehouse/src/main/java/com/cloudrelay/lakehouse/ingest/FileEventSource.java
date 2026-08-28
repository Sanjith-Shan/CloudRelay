package com.cloudrelay.lakehouse.ingest;

import com.cloudrelay.lakehouse.LakehouseOptions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.current_timestamp;
import static org.apache.spark.sql.functions.input_file_name;
import static org.apache.spark.sql.functions.lit;

/**
 * Reads newline delimited event JSON from a landing directory.
 *
 * <p>This exists for two jobs that should not need a broker: the restart tests,
 * where {@code maxFilesPerTrigger} makes batch boundaries deterministic and a
 * kill can be aimed at a known point in the stream, and the compaction
 * benchmark, which needs to replay the same bytes many times to compare
 * before and after numbers on equal input.
 *
 * <p>It reports {@code -1} for partition and offset rather than inventing them.
 * A file source has no offsets, and a fabricated one would look like a real
 * Kafka coordinate to anybody reading bronze later.
 */
public final class FileEventSource implements EventSource {

    private final LakehouseOptions options;
    private final int maxFilesPerTrigger;

    public FileEventSource(LakehouseOptions options, int maxFilesPerTrigger) {
        this.options = options;
        this.maxFilesPerTrigger = maxFilesPerTrigger;
    }

    @Override
    public Dataset<Row> readStream(SparkSession spark) {
        return spark.readStream()
                .format("text")
                .option("maxFilesPerTrigger", maxFilesPerTrigger)
                .load(options.landingPath())
                .select(
                        col("value").as(RAW_VALUE),
                        input_file_name().as(SOURCE_KEY),
                        lit(-1).as(SOURCE_PARTITION),
                        lit(-1L).as(SOURCE_OFFSET),
                        current_timestamp().as(SOURCE_TIMESTAMP));
    }

    @Override
    public String describe() {
        return "file:" + options.landingPath();
    }
}
