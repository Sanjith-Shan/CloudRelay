package com.cloudrelay.lakehouse.query;

import com.cloudrelay.lakehouse.DeltaTables;
import io.delta.tables.DeltaTable;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

/**
 * Reading a table as it stood at an earlier commit.
 *
 * <p>This falls out of how Delta stores a table rather than being a feature
 * bolted on top of it. The table is not a directory of files, it is an ordered
 * log of commits, each one recording which files were added and which were
 * removed. "The current table" is the log replayed to the end; "the table at
 * version 7" is the same replay stopped early. Nothing is copied and nothing
 * is snapshotted.
 *
 * <p>The practical value is that a bad deploy that wrote wrong rows at 03:00 is
 * recoverable by reading the version before it and rewriting, instead of by
 * restoring a backup. The limit is real too: VACUUM deletes the files older
 * versions point at, so history reaches back exactly as far as the retention
 * window and not one commit further.
 */
public final class TimeTravel {

    public record Version(long version, String operation, long rowCount, String timestamp) {
    }

    private TimeTravel() {
    }

    /** Every version of a table with the row count it held at that point. */
    public static List<Version> history(SparkSession spark, String path, int limit) {
        List<Row> commits = DeltaTable.forPath(spark, path)
                .history(limit)
                .select("version", "timestamp", "operation")
                .collectAsList();

        List<Version> out = new ArrayList<>();
        for (Row commit : commits) {
            long version = commit.getLong(0);
            long count = DeltaTables.readVersion(spark, path, version).count();
            out.add(new Version(version, commit.getString(2), count,
                    String.valueOf(commit.get(1))));
        }
        return out;
    }

    public static String render(String path, List<Version> versions) {
        StringBuilder sb = new StringBuilder();
        sb.append("== Delta history for ").append(path).append(" ==\n");
        sb.append(String.format("%-9s %-22s %-24s %s%n",
                "version", "operation", "timestamp", "rows visible"));
        for (Version v : versions) {
            sb.append(String.format("%-9d %-22s %-24s %d%n",
                    v.version(), v.operation(), v.timestamp(), v.rowCount()));
        }
        return sb.toString();
    }
}
