package com.cloudrelay.lakehouse.transform;

import com.cloudrelay.lakehouse.DeltaTables;
import com.cloudrelay.lakehouse.LakehouseOptions;
import com.cloudrelay.lakehouse.SessionEventSchema;
import com.cloudrelay.lakehouse.StreamingSupport;
import com.cloudrelay.lakehouse.ingest.EventSource;
import io.delta.tables.DeltaTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.storage.StorageLevel;

import java.sql.Date;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.current_timestamp;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.not;
import static org.apache.spark.sql.functions.sha2;
import static org.apache.spark.sql.functions.size;
import static org.apache.spark.sql.functions.to_date;

/**
 * Silver: typed, validated, deduplicated.
 *
 * <p>Three things happen here that bronze deliberately refused to do.
 *
 * <p><b>Parsing against a declared schema.</b> {@link SessionEventSchema} is the
 * contract, not a sample of the data. Spark's PERMISSIVE mode turns anything
 * that does not fit into nulls instead of raising, which is why the first
 * quality rule exists to catch exactly that.
 *
 * <p><b>Deduplication by MERGE on {@code event_id}.</b> See below; this is the
 * design decision in this class worth reading.
 *
 * <p><b>Routing bad rows.</b> Valid rows go to silver, the rest to quarantine
 * with the names of the rules that rejected them. Both writes happen inside one
 * {@code foreachBatch} rather than as two independent streaming queries: two
 * queries would read the same bronze stream twice under two checkpoints, and a
 * crash between them would leave the tables at different offsets with nothing
 * recording that they disagreed.
 *
 * <h2>Why the deduplication is a MERGE and not a watermark</h2>
 *
 * <p>The obvious way to deduplicate a stream is
 * {@code withWatermark(...).dropDuplicatesWithinWatermark("event_id")}. It is
 * the documented approach, its state is bounded, and it is what this class did
 * first. It also silently lost data, and the way it did so is worth keeping on
 * the record.
 *
 * <p>A watermark tracks the maximum event time seen and discards anything that
 * arrives more than the configured delay behind it. Under a small
 * {@code maxFilesPerTrigger} — or any backfill, or any replay from bronze —
 * a single early batch can contain events from across the whole time range. The
 * watermark jumps almost to the maximum immediately, and from the next batch
 * onward the majority of events are older than it and are dropped as late. In
 * this project's own compaction test, that cost 8,276 ingested events all but
 * 354 of them. Nothing failed. The counts were simply wrong.
 *
 * <p>The watermark is not the wrong tool because it is late-intolerant; it is
 * the wrong tool because <em>ordering is not a property this source has</em>.
 * Kafka guarantees order within a partition, not across them, and replaying a
 * Delta table guarantees nothing at all.
 *
 * <p>So the identity check lives in the table instead of in Spark's state store.
 * A MERGE with only a {@code WHEN NOT MATCHED THEN INSERT} clause is an
 * insert-if-absent against silver's own {@code event_id}, which makes it correct
 * regardless of arrival order, arbitrarily late data, batch replays after a
 * crash, and producer retries, all with one mechanism. It also leaves the query
 * stateless — there is no state store to recover, so a restart has nothing to
 * restore beyond its offsets.
 *
 * <p>The cost is a join against silver on every micro-batch instead of a lookup
 * in memory. That is paid down by pruning: silver is partitioned by
 * {@code event_date} and the merge condition carries the batch's own date range,
 * so Delta reads the partitions the batch could collide with rather than the
 * table. Correctness first, and then make it cheap — not the other way round.
 */
public final class SilverTransform {

    public static final String QUERY_NAME = "silver-transform";

    private static final String QUARANTINE_APP_ID = "cloudrelay-quarantine";

    /** Dedup identity: the producer's event id, or a content hash when absent. */
    private static final String DEDUP_KEY = "dedup_key";

    private static final String[] INTERNAL_COLUMNS =
            {DEDUP_KEY, DataQuality.FAILURES_COLUMN, EventSource.RAW_VALUE};

    private final LakehouseOptions options;

    public SilverTransform(LakehouseOptions options) {
        this.options = options;
    }

    /** Flattens the bronze envelope into the silver column set. Pure, so it is unit testable. */
    public static Dataset<Row> parse(Dataset<Row> bronze) {
        Dataset<Row> parsed = bronze.withColumn(
                "envelope",
                from_json(col(EventSource.RAW_VALUE), SessionEventSchema.envelope()));

        return parsed.select(
                col("envelope.eventId").as("event_id"),
                // A lenient ISO-8601 cast rather than an explicit format: the
                // service emits Instant.toString(), which omits the fractional
                // part entirely when it happens to be zero, so no single
                // pattern matches every value. An unparseable string becomes
                // null here and is caught by event_time_parses, which keeps the
                // failure visible instead of silently dropping it.
                col("envelope.eventTime").cast("timestamp").as("event_time"),
                col("envelope.type").as("event_type"),
                col("envelope.sessionCode").as("session_code"),
                col("envelope.schemaVersion").as("schema_version"),
                col("envelope.session.gameId").as("game_id"),
                col("envelope.session.region").as("region"),
                col("envelope.session.state").as("session_state"),
                col("envelope.session.host.playerId").as("host_player_id"),
                col("envelope.session.currentPlayerCount").as("player_count"),
                col("envelope.session.maxPlayers").as("max_players"),
                col("envelope.session.minPlayersToStart").as("min_players_to_start"),
                col("envelope.session.isFull").as("is_full"),
                col("envelope.session.uptimeSeconds").as("uptime_seconds"),
                col("envelope.session.createdAt").cast("timestamp").as("session_created_at"),
                col("envelope.session.updatedAt").cast("timestamp").as("session_updated_at"),
                col(EventSource.RAW_VALUE),
                col(EventSource.SOURCE_PARTITION),
                col(EventSource.SOURCE_OFFSET),
                col("ingest_time"));
    }

    /** Parse, validate, and add the deduplication key. */
    public static Dataset<Row> prepare(Dataset<Row> bronze) {
        return DataQuality.annotate(parse(bronze))
                .withColumn(DEDUP_KEY,
                        coalesce(col("event_id"), sha2(col(EventSource.RAW_VALUE), 256)));
    }

    public StreamingQuery start(SparkSession spark) throws TimeoutException {
        Dataset<Row> bronze = spark.readStream()
                .format("delta")
                .option(LakehouseOptions.MAX_FILES_PER_TRIGGER, options.maxFilesPerTrigger())
                .load(options.bronzePath());

        return StreamingSupport.configure(
                        prepare(bronze).writeStream()
                                .outputMode("append")
                                .foreachBatch(this::writeBatch),
                        options, QUERY_NAME)
                .start();
    }

    /**
     * Writes one micro-batch to both sinks.
     *
     * <p>{@code foreachBatch} gives at-least-once delivery: a driver failure
     * after the write but before the offset commit re-runs the same batch id
     * with the same rows. Silver absorbs that in the merge, which is an
     * insert-if-absent and so is naturally idempotent. Quarantine cannot use
     * the same trick — a malformed row has no reliable identity to match on —
     * so it uses Delta's {@code txnAppId}/{@code txnVersion} markers instead:
     * the table log remembers the highest batch id committed for an app id and
     * skips a replay of it.
     */
    private void writeBatch(Dataset<Row> batch, long batchId) {
        batch.persist(StorageLevel.MEMORY_AND_DISK());
        try {
            Dataset<Row> valid = batch
                    .filter(DataQuality.isValid())
                    // Collapse duplicates inside this batch before the merge.
                    // A MERGE whose source contains the same key twice fails
                    // outright rather than picking one, and a producer retry
                    // landing in the same batch as its original is the ordinary
                    // case, not a rare one.
                    .dropDuplicates(DEDUP_KEY)
                    .drop(INTERNAL_COLUMNS)
                    .withColumn("event_date", to_date(col("event_time")));

            mergeIntoSilver(batch.sparkSession(), valid);
            writeQuarantine(batch, batchId);
        } finally {
            batch.unpersist();
        }
    }

    private void mergeIntoSilver(SparkSession spark, Dataset<Row> valid) {
        if (valid.isEmpty()) {
            return;
        }
        DeltaTables.ensureExists(spark, options.silverPath(), valid.schema(),
                java.util.List.of("event_date"));

        // The date range this batch actually covers, added to the merge
        // condition so Delta can eliminate whole partitions instead of scanning
        // silver. Without it the merge is correct and progressively slower.
        Row bounds = valid.agg(min("event_date"), max("event_date")).first();
        Date lo = bounds.getDate(0);
        Date hi = bounds.getDate(1);

        String condition = "t.event_id = s.event_id"
                + " AND t.event_date BETWEEN date'" + lo + "' AND date'" + hi + "'";

        DeltaTable.forPath(spark, options.silverPath()).as("t")
                .merge(valid.as("s"), condition)
                .whenNotMatched().insertAll()
                .execute();
    }

    private void writeQuarantine(Dataset<Row> batch, long batchId) {
        Dataset<Row> quarantined = batch
                .filter(not(DataQuality.isValid()))
                .drop(DEDUP_KEY)
                .withColumn("quarantined_at", current_timestamp())
                .withColumn("failure_count", size(col(DataQuality.FAILURES_COLUMN)));

        if (quarantined.isEmpty()) {
            return;
        }
        quarantined.write()
                .format("delta")
                .mode(SaveMode.Append)
                .option("txnAppId", QUARANTINE_APP_ID)
                .option("txnVersion", batchId)
                .option("mergeSchema", "true")
                .save(options.quarantinePath());
    }

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
}
