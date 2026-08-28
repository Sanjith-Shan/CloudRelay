package com.cloudrelay.lakehouse.maintenance;

import org.apache.spark.sql.SparkSession;

import java.util.List;

/**
 * OPTIMIZE, and why a streaming table needs it.
 *
 * <p>A streaming sink writes at least one file per micro-batch per partition,
 * and a batch that carried forty rows still produces a file. Run for an hour at
 * a ten second trigger and the table is thousands of files of a few kilobytes
 * each. Every one of them costs a metadata lookup, an open, and a footer read
 * before a single row comes back, so query time ends up governed by file count
 * rather than by data volume — a table can get slower while staying the same
 * size. This is the small file problem, and streaming ingestion causes it by
 * construction, not by misconfiguration.
 *
 * <p>OPTIMIZE rewrites those files into a few large ones and commits the swap
 * atomically. Readers see the old set until the commit and the new set after
 * it; nothing has to stop.
 *
 * <p>ZORDER goes further. Parquet keeps per-file min/max statistics, so a filter
 * can skip a file whose range cannot contain a match. That only helps if
 * related rows share files, and rows arrive ordered by time, not by the columns
 * anyone filters on. Z-ordering interleaves the bits of the chosen columns to
 * lay out rows so that values close in several dimensions land close on disk,
 * which is what turns those statistics into skipped files.
 */
public final class CompactionJob {

    private CompactionJob() {
    }

    /** Plain compaction: rewrite small files into large ones. */
    public static void optimize(SparkSession spark, String path) {
        spark.sql("OPTIMIZE delta.`" + path + "`").collect();
    }

    /** Compaction plus a Z-order layout on the columns queries filter by. */
    public static void optimizeZOrdered(SparkSession spark, String path, List<String> columns) {
        if (columns.isEmpty()) {
            optimize(spark, path);
            return;
        }
        spark.sql("OPTIMIZE delta.`" + path + "` ZORDER BY ("
                + String.join(", ", columns) + ")").collect();
    }

    /**
     * Deletes files no longer referenced by the log and older than the
     * retention window.
     *
     * <p>The default retention is seven days and it is not arbitrary: a reader
     * that started before an OPTIMIZE is still reading the old files, and time
     * travel to a version before the rewrite needs them too. Vacuuming below
     * the window breaks both. This project sets
     * {@code retentionDurationCheck.enabled=false} so a demo can vacuum at zero
     * hours and show the space actually come back — which is a thing to do on a
     * laptop and not on a table anyone depends on.
     */
    public static void vacuum(SparkSession spark, String path, int retentionHours) {
        spark.sql("VACUUM delta.`" + path + "` RETAIN " + retentionHours + " HOURS").collect();
    }
}
