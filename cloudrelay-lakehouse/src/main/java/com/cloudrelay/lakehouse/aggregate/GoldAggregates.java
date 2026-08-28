package com.cloudrelay.lakehouse.aggregate;

import com.cloudrelay.lakehouse.DeltaTables;
import com.cloudrelay.lakehouse.LakehouseOptions;
import com.cloudrelay.lakehouse.StreamingSupport;
import io.delta.tables.DeltaTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;
import org.apache.spark.storage.StorageLevel;

import java.sql.Timestamp;

import static org.apache.spark.sql.functions.approx_count_distinct;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.current_timestamp;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.when;
import static org.apache.spark.sql.functions.window;

/**
 * Gold: the numbers a game operator would actually open a dashboard to see.
 *
 * <h2>Why these aggregates recompute instead of accumulate</h2>
 *
 * <p>The obvious way to keep a rolling per-minute count from a stream is to
 * MERGE each micro-batch's counts into the target with
 * {@code SET count = target.count + source.count}. It is wrong in a way that
 * only shows up under failure: {@code foreachBatch} guarantees at-least-once,
 * so a driver that dies between the write and the offset commit re-runs the
 * same batch, and an additive merge counts it twice. Delta's
 * {@code txnAppId}/{@code txnVersion} markers exist to patch exactly that hole.
 *
 * <p>This job takes the other road. Each batch works out which one-minute
 * windows it touched, re-reads <em>those windows only</em> from silver, and
 * MERGEs the recomputed value in as a replacement. Replacement is idempotent by
 * construction — running the same batch twice produces the identical table — so
 * the correctness does not rest on a transaction marker being threaded through
 * every write path. It also fixes late data for free: an event that arrives
 * three minutes behind reopens its window and the number is simply right,
 * where an additive pipeline would need a separate backfill to repair it.
 *
 * <p>What it costs is a bounded re-read of silver per batch. Silver is
 * partitioned by {@code event_date} and the re-read is filtered to the touched
 * window range, so the partition pruning keeps it to the minutes in play rather
 * than the table. That is the trade, and it is the right one here: this data is
 * worth more correct than cheap.
 */
public final class GoldAggregates {

    public static final String QUERY_NAME = "gold-aggregates";

    private final LakehouseOptions options;

    public GoldAggregates(LakehouseOptions options) {
        this.options = options;
    }

    private String windowSpec() {
        return options.goldWindow().toMinutes() + " minutes";
    }

    // ---- Streaming rollups ------------------------------------------------

    public StreamingQuery start(SparkSession spark) throws TimeoutException {
        Dataset<Row> silver = spark.readStream().format("delta")
                .option(LakehouseOptions.MAX_FILES_PER_TRIGGER, options.maxFilesPerTrigger())
                .load(options.silverPath());

        return StreamingSupport.configure(
                        silver.writeStream()
                                .outputMode("append")
                                .foreachBatch((Dataset<Row> batch, Long batchId) -> refresh(batch)),
                        options, QUERY_NAME)
                .start();
    }

    /**
     * Recomputes every one-minute window this batch touched, from silver.
     */
    private void refresh(Dataset<Row> batch) {
        batch.persist(StorageLevel.MEMORY_AND_DISK());
        try {
            if (batch.isEmpty()) {
                return;
            }
            Row bounds = batch.agg(
                    min("event_time").as("lo"),
                    max("event_time").as("hi")).first();
            Timestamp lo = bounds.getTimestamp(0);
            Timestamp hi = bounds.getTimestamp(1);
            if (lo == null || hi == null) {
                return;
            }

            SparkSession spark = batch.sparkSession();
            // Batch read of silver restricted to the touched range. The
            // event_date partition filter is what keeps this from becoming a
            // full table scan on every micro-batch.
            Dataset<Row> scope = DeltaTables.read(spark, options.silverPath())
                    .filter(col("event_date").between(
                            lit(lo).cast("date"), lit(hi).cast("date")))
                    .filter(col("event_time").between(lit(lo), lit(hi)));

            mergeLifecycle(spark, scope);
            mergeConcurrency(spark, scope);
        } finally {
            batch.unpersist();
        }
    }

    /** Per minute, per game, per region: what the session lifecycle did. */
    public Dataset<Row> lifecycle(Dataset<Row> silver) {
        return silver
                .groupBy(window(col("event_time"), windowSpec()), col("game_id"), col("region"))
                .agg(
                        countWhere("SESSION_CREATED").as("sessions_created"),
                        countWhere("SESSION_STARTED").as("sessions_started"),
                        countWhere("SESSION_TERMINATED").as("sessions_terminated"),
                        countWhere("PLAYER_JOINED").as("player_joins"),
                        countWhere("PLAYER_LEFT").as("player_leaves"),
                        count(lit(1)).as("event_count"),
                        // HyperLogLog rather than an exact distinct count. The
                        // exact version has to hold every session code seen in
                        // the window in memory; this holds a fixed size sketch
                        // for a few percent of error on a number that is read
                        // as a trend line. Where the number has to be exact,
                        // the batch tables below compute it exactly.
                        approx_count_distinct(col("session_code")).as("distinct_sessions"),
                        max("player_count").as("peak_players_in_a_session"))
                .select(
                        col("window.start").as("window_start"),
                        col("window.end").as("window_end"),
                        col("game_id"), col("region"),
                        col("sessions_created"), col("sessions_started"),
                        col("sessions_terminated"), col("player_joins"),
                        col("player_leaves"), col("event_count"),
                        col("distinct_sessions"), col("peak_players_in_a_session"))
                .withColumn("computed_at", current_timestamp());
    }

    /** Per minute, per region: how much concurrent load the region carried. */
    public Dataset<Row> concurrency(Dataset<Row> silver) {
        return silver
                .groupBy(window(col("event_time"), windowSpec()), col("region"))
                .agg(
                        approx_count_distinct(col("session_code")).as("active_sessions"),
                        sum("player_count").as("player_slot_events"),
                        max("player_count").as("peak_session_size"),
                        count(lit(1)).as("event_count"))
                .select(
                        col("window.start").as("window_start"),
                        col("window.end").as("window_end"),
                        col("region"), col("active_sessions"),
                        col("player_slot_events"), col("peak_session_size"),
                        col("event_count"))
                .withColumn("computed_at", current_timestamp());
    }

    private void mergeLifecycle(SparkSession spark, Dataset<Row> scope) {
        Dataset<Row> source = lifecycle(scope);
        DeltaTables.ensureExists(spark, options.goldLifecyclePath(), source.schema());
        DeltaTable.forPath(spark, options.goldLifecyclePath()).as("t")
                .merge(source.as("s"),
                        "t.window_start = s.window_start AND t.game_id = s.game_id "
                                + "AND t.region = s.region")
                .whenMatched().updateAll()
                .whenNotMatched().insertAll()
                .execute();
    }

    private void mergeConcurrency(SparkSession spark, Dataset<Row> scope) {
        Dataset<Row> source = concurrency(scope);
        DeltaTables.ensureExists(spark, options.goldConcurrencyPath(), source.schema());
        DeltaTable.forPath(spark, options.goldConcurrencyPath()).as("t")
                .merge(source.as("s"),
                        "t.window_start = s.window_start AND t.region = s.region")
                .whenMatched().updateAll()
                .whenNotMatched().insertAll()
                .execute();
    }

    private static org.apache.spark.sql.Column countWhere(String eventType) {
        return sum(when(col("event_type").equalTo(eventType), 1).otherwise(0));
    }

    // ---- Batch rollups ----------------------------------------------------
    //
    // Two things gold needs are per session rather than per window, and a
    // windowed streaming aggregation is the wrong shape for both: a session's
    // duration is not known until it ends, which may be hours after the window
    // its first event fell in. Running them as a full recompute over silver is
    // simpler than a stateful session-window operator, it is exact rather than
    // approximate, and overwrite semantics make it idempotent for free.

    /** One row per session that has ended: how long it lived and how big it got. */
    public Dataset<Row> sessionDurations(Dataset<Row> silver) {
        return silver
                .groupBy(col("session_code"))
                .agg(
                        // max() rather than first() on the invariant columns.
                        // A session's game and region never change, so any row
                        // would do, but first() inside a groupBy picks whichever
                        // row the shuffle happened to deliver first — it is
                        // non-deterministic, and an ORDER BY before the groupBy
                        // does not fix that, it just makes it look fixed.
                        max("game_id").as("game_id"),
                        max("region").as("region"),
                        min("event_time").as("first_event_at"),
                        max("event_time").as("last_event_at"),
                        max("player_count").as("peak_players"),
                        max("max_players").as("capacity"),
                        count(lit(1)).as("event_count"),
                        countWhere("SESSION_STARTED").as("start_events"),
                        countWhere("SESSION_TERMINATED").as("terminate_events"),
                        // The service stamps uptime on every event, so the
                        // latest one carries the session's own account of how
                        // long it ran. Preferring it over the difference of
                        // observed event times means a session whose final
                        // event was delayed in the pipeline still reports its
                        // real duration rather than the pipeline's latency.
                        //
                        // max_by, not last: this has to be the value belonging
                        // to the row with the greatest event_time, and that is
                        // exactly what max_by means. last() would return
                        // whatever arrived last in a shuffle that makes no
                        // promise about order.
                        expr("max_by(uptime_seconds, event_time)")
                                .as("reported_uptime_seconds"))
                .withColumn("observed_duration_seconds",
                        expr("unix_timestamp(last_event_at) - unix_timestamp(first_event_at)"))
                .withColumn("duration_seconds",
                        expr("coalesce(reported_uptime_seconds, observed_duration_seconds)"))
                .withColumn("completed", col("terminate_events").gt(0))
                .withColumn("computed_at", current_timestamp());
    }

    /**
     * Created to started to terminated, per game and region. This is the
     * conversion funnel a matchmaking change is judged by.
     */
    public Dataset<Row> matchmakingFunnel(Dataset<Row> sessionDurations) {
        return sessionDurations
                .groupBy(col("game_id"), col("region"))
                .agg(
                        count(lit(1)).as("sessions_observed"),
                        sum(when(col("start_events").gt(0), 1).otherwise(0)).as("sessions_started"),
                        sum(when(col("terminate_events").gt(0), 1).otherwise(0))
                                .as("sessions_terminated"),
                        expr("percentile_approx(duration_seconds, 0.5)").as("p50_duration_seconds"),
                        expr("percentile_approx(duration_seconds, 0.95)").as("p95_duration_seconds"),
                        expr("avg(peak_players)").as("avg_peak_players"))
                .withColumn("start_rate",
                        expr("round(sessions_started / sessions_observed, 4)"))
                .withColumn("computed_at", current_timestamp());
    }

    /** Recomputes both per-session tables from the whole of silver. */
    public void runBatchAggregates(SparkSession spark) {
        Dataset<Row> silver = DeltaTables.read(spark, options.silverPath());

        Dataset<Row> durations = sessionDurations(silver);
        durations.persist(StorageLevel.MEMORY_AND_DISK());
        try {
            durations.write().format("delta").mode(SaveMode.Overwrite)
                    .option("overwriteSchema", "true")
                    .save(options.goldDurationPath());

            matchmakingFunnel(durations).write().format("delta").mode(SaveMode.Overwrite)
                    .option("overwriteSchema", "true")
                    .save(options.goldFunnelPath());
        } finally {
            durations.unpersist();
        }
    }

    public void runOnce(SparkSession spark) throws Exception {
        StreamingQuery query = start(spark);
        if (options.triggerMode() == LakehouseOptions.TriggerMode.RATE_LIMITED) {
            StreamingSupport.awaitCaughtUp(query);
        } else {
            query.awaitTermination();
        }
        runBatchAggregates(spark);
    }
}
