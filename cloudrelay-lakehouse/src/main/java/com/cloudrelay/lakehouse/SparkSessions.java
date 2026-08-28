package com.cloudrelay.lakehouse;

import org.apache.spark.sql.SparkSession;

/** Builds the one {@link SparkSession} shape every job in this module expects. */
public final class SparkSessions {

    private SparkSessions() {
    }

    public static SparkSession create(String appName, LakehouseOptions options) {
        SparkSession.Builder builder = SparkSession.builder()
                .appName(appName)
                .master(options.sparkMaster());
        options.sparkConf().forEach(builder::config);
        SparkSession spark = builder.getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        return spark;
    }
}
