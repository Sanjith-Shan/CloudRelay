package com.cloudrelay.lakehouse.ingest;

import com.cloudrelay.lakehouse.LakehouseOptions;
import com.cloudrelay.lakehouse.StreamingSupport;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.current_timestamp;
import static org.apache.spark.sql.functions.length;
import static org.apache.spark.sql.functions.to_date;

/**
 * Bronze: the raw envelope, appended and never edited.
 *
 * <p>Nothing here parses the payload. Bronze's one job is to be the replayable
 * record of what actually arrived, and any parsing decision made at this layer
 * is a decision that cannot be revised later without going back to Kafka, which
 * has a retention window. Silver can be dropped and rebuilt from bronze as many
 * times as the schema changes; bronze can only be rebuilt from a broker that
 * may already have aged the data out.
 *
 * <p>The one thing it does add is provenance: ingest time, and the source
 * coordinates the row came from.
 */
public final class BronzeIngestion {

    private final LakehouseOptions options;
    private final EventSource source;

    public BronzeIngestion(LakehouseOptions options, EventSource source) {
        this.options = options;
        this.source = source;
    }

    public static final String QUERY_NAME = "bronze-ingest";

    public StreamingQuery start(SparkSession spark) throws TimeoutException {
        Dataset<Row> raw = source.readStream(spark);

        Dataset<Row> bronze = raw
                // An empty line is a file source artefact, not an event. It is
                // the one thing filtered here, because it carries no envelope to
                // quarantine and would only ever become a null row in silver.
                .filter(length(col(EventSource.RAW_VALUE)).gt(0))
                .withColumn("ingest_time", current_timestamp())
                // Partitioned on the broker's append date, not on a date parsed
                // out of the payload. Reading the payload to decide the
                // partition would make bronze depend on the schema it exists to
                // be independent of, and a malformed event would then have
                // nowhere to land.
                .withColumn("ingest_date", to_date(col(EventSource.SOURCE_TIMESTAMP)));

        return StreamingSupport.configure(
                        bronze.writeStream()
                                .format("delta")
                                .outputMode("append")
                                .partitionBy("ingest_date"),
                        options, QUERY_NAME)
                .start(options.bronzePath());
    }

    /** Runs the ingest to completion. Only valid under {@code AVAILABLE_NOW}. */
    public void runOnce(SparkSession spark) throws Exception {
        StreamingQuery query = start(spark);
        // AvailableNow terminates by itself; a rate limited replay has to be
        // told when it has caught up, because its trigger never ends.
        if (options.triggerMode() == LakehouseOptions.TriggerMode.RATE_LIMITED) {
            StreamingSupport.awaitCaughtUp(query);
        } else {
            query.awaitTermination();
        }
    }

    /**
     * Stops after exactly {@code batches} micro-batches, leaving the checkpoint
     * mid-stream. This is how the restart test manufactures a crash at a known
     * point rather than hoping a kill lands somewhere interesting.
     */
    public StreamingQuery startBounded(SparkSession spark, int batches) throws Exception {
        Dataset<Row> bronze = source.readStream(spark)
                .filter(length(col(EventSource.RAW_VALUE)).gt(0))
                .withColumn("ingest_time", current_timestamp())
                .withColumn("ingest_date", to_date(col(EventSource.SOURCE_TIMESTAMP)));

        StreamingQuery query = bronze.writeStream()
                .format("delta")
                .outputMode("append")
                .partitionBy("ingest_date")
                .queryName(QUERY_NAME)
                .option("checkpointLocation", options.checkpointPath(QUERY_NAME))
                .trigger(Trigger.ProcessingTime(0))
                .start(options.bronzePath());

        while (query.isActive() && query.recentProgress().length < batches) {
            Thread.sleep(50);
        }
        query.stop();
        return query;
    }
}
