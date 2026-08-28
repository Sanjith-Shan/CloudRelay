package com.cloudrelay.lakehouse.ingest;

import com.cloudrelay.lakehouse.LakehouseOptions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

/**
 * Reads the {@code cloudrelay.session-events} topic.
 *
 * <p>The service keys every record by session code, so all events for one
 * session land on one partition and arrive in the order they happened. Silver
 * does not depend on that ordering, but it makes the raw topic readable when
 * something has gone wrong and somebody is reading offsets by hand.
 */
public final class KafkaEventSource implements EventSource {

    private final LakehouseOptions options;

    public KafkaEventSource(LakehouseOptions options) {
        this.options = options;
    }

    @Override
    public Dataset<Row> readStream(SparkSession spark) {
        return spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", options.kafkaBootstrapServers())
                .option("subscribe", options.kafkaTopic())
                .option("startingOffsets", options.startingOffsets())
                // A retention expiry between two runs deletes offsets the
                // checkpoint still refers to. Failing there would wedge the
                // pipeline permanently on data that no longer exists, so skip
                // the gap and keep going; bronze records the offsets, which is
                // what makes the gap visible afterwards.
                .option("failOnDataLoss", "false")
                .load()
                .select(
                        col("value").cast("string").as(RAW_VALUE),
                        col("key").cast("string").as(SOURCE_KEY),
                        col("partition").as(SOURCE_PARTITION),
                        col("offset").as(SOURCE_OFFSET),
                        col("timestamp").as(SOURCE_TIMESTAMP));
    }

    @Override
    public String describe() {
        return "kafka:" + options.kafkaTopic();
    }
}
