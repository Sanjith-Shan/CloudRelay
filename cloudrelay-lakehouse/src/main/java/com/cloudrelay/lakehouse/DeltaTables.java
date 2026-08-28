package com.cloudrelay.lakehouse;

import io.delta.tables.DeltaTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

/** Small helpers for Delta tables that several jobs need. */
public final class DeltaTables {

    private DeltaTables() {
    }

    /**
     * Creates the table empty if it is not there yet.
     *
     * <p>MERGE needs a target that already exists, and the first micro-batch of
     * a fresh pipeline has nothing to merge into. Creating it empty up front is
     * better than branching between "insert everything" and "merge" on every
     * batch: one code path, and the table's schema is pinned from the start
     * rather than being whatever the first batch happened to contain.
     */
    public static void ensureExists(SparkSession spark, String path, StructType schema) {
        ensureExists(spark, path, schema, java.util.List.of());
    }

    public static void ensureExists(SparkSession spark, String path, StructType schema,
                                    java.util.List<String> partitionColumns) {
        if (DeltaTable.isDeltaTable(spark, path)) {
            return;
        }
        var writer = spark.createDataFrame(java.util.List.of(), schema)
                .write().format("delta").mode(SaveMode.ErrorIfExists);
        if (!partitionColumns.isEmpty()) {
            writer = writer.partitionBy(partitionColumns.toArray(String[]::new));
        }
        writer.save(path);
    }

    public static boolean exists(SparkSession spark, String path) {
        return DeltaTable.isDeltaTable(spark, path);
    }

    public static Dataset<Row> read(SparkSession spark, String path) {
        return spark.read().format("delta").load(path);
    }

    /** Reads a table as it stood at a given commit version. */
    public static Dataset<Row> readVersion(SparkSession spark, String path, long version) {
        return spark.read().format("delta").option("versionAsOf", version).load(path);
    }

    /** The version number of the table's most recent commit. */
    public static long latestVersion(SparkSession spark, String path) {
        return DeltaTable.forPath(spark, path).history(1)
                .select("version").first().getLong(0);
    }
}
