package com.cloudrelay.lakehouse.maintenance;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * The physical shape of a Delta table: how many files hold it and how big they are.
 *
 * <p>Read from {@code DESCRIBE DETAIL}, which reports the files the transaction
 * log currently considers live. Counting {@code *.parquet} on disk would be
 * wrong after an OPTIMIZE, because the compacted-away originals stay on disk
 * until VACUUM removes them — the whole point of the log is that "what is in
 * the table" and "what is in the directory" are different questions.
 */
public record TableStats(long numFiles, long sizeInBytes) {

    public static TableStats of(SparkSession spark, String path) {
        Row detail = spark.sql("DESCRIBE DETAIL delta.`" + path + "`").first();
        return new TableStats(
                detail.getAs("numFiles"),
                detail.getAs("sizeInBytes"));
    }

    public long avgFileBytes() {
        return numFiles == 0 ? 0 : sizeInBytes / numFiles;
    }

    public String humanSize() {
        return humanBytes(sizeInBytes);
    }

    public String humanAvgFile() {
        return humanBytes(avgFileBytes());
    }

    static String humanBytes(long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        }
        if (bytes < 1024 * 1024) {
            return String.format("%.1f KB", bytes / 1024.0);
        }
        return String.format("%.1f MB", bytes / (1024.0 * 1024.0));
    }
}
