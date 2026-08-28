package com.cloudrelay.lakehouse.ingest;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Where raw session events come from.
 *
 * <p>Bronze is defined by what it does to the bytes, not by where the bytes came
 * from, so the source is a strategy the job is handed. Kafka is the real one;
 * the file source exists so the pipeline, its tests and the compaction
 * benchmark can run without a broker. Both must produce the same columns:
 *
 * <ul>
 *   <li>{@code raw_value}        the event JSON, untouched
 *   <li>{@code source_key}       partition key, or the file name where there is none
 *   <li>{@code source_partition} Kafka partition, or -1
 *   <li>{@code source_offset}    Kafka offset, or -1
 *   <li>{@code source_timestamp} broker append time, or file modification time
 * </ul>
 */
public interface EventSource {

    /** Column names every implementation is required to produce. */
    String RAW_VALUE = "raw_value";
    String SOURCE_KEY = "source_key";
    String SOURCE_PARTITION = "source_partition";
    String SOURCE_OFFSET = "source_offset";
    String SOURCE_TIMESTAMP = "source_timestamp";

    Dataset<Row> readStream(SparkSession spark);

    /** Human readable name, used in the streaming query name and checkpoint path. */
    String describe();
}
