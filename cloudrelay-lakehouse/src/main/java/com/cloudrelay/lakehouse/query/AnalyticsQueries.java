package com.cloudrelay.lakehouse.query;

import com.cloudrelay.lakehouse.DeltaTables;
import com.cloudrelay.lakehouse.LakehouseOptions;
import com.cloudrelay.lakehouse.transform.DataQuality;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The questions the gold tables exist to answer, written as SQL.
 *
 * <p>Gold's job is to make the operator's question a one line query rather than
 * a join across three tables and a window function. If a question here needs
 * more than a {@code SELECT} with an {@code ORDER BY}, that is a signal the
 * aggregation belongs upstream in gold and not in the dashboard.
 */
public final class AnalyticsQueries {

    private final LakehouseOptions options;

    public AnalyticsQueries(LakehouseOptions options) {
        this.options = options;
    }

    /** Registers the lakehouse tables as temp views so the SQL below reads plainly. */
    public void registerViews(SparkSession spark) {
        registerIfPresent(spark, "bronze_session_events", options.bronzePath());
        registerIfPresent(spark, "silver_session_events", options.silverPath());
        registerQuarantine(spark);
        registerIfPresent(spark, "gold_session_lifecycle_1m", options.goldLifecyclePath());
        registerIfPresent(spark, "gold_region_concurrency_1m", options.goldConcurrencyPath());
        registerIfPresent(spark, "gold_session_duration", options.goldDurationPath());
        registerIfPresent(spark, "gold_matchmaking_funnel", options.goldFunnelPath());
    }

    private void registerIfPresent(SparkSession spark, String view, String path) {
        if (DeltaTables.exists(spark, path)) {
            DeltaTables.read(spark, path).createOrReplaceTempView(view);
        }
    }

    /**
     * Registers the quarantine view, substituting an empty one when the table
     * does not exist yet.
     *
     * <p>The table is only created the first time something is rejected, so on
     * a pipeline that has never seen a malformed event it is simply absent —
     * and without this, every query mentioning it fails. That means the health
     * summary is unavailable exactly when the pipeline is healthiest, which is
     * the wrong way round. An empty view gives the true answer, which is zero.
     */
    private void registerQuarantine(SparkSession spark) {
        if (DeltaTables.exists(spark, options.quarantinePath())) {
            DeltaTables.read(spark, options.quarantinePath())
                    .createOrReplaceTempView("silver_quarantine");
            return;
        }
        StructType empty = new StructType()
                .add("event_id", DataTypes.StringType)
                .add("session_code", DataTypes.StringType)
                .add(DataQuality.FAILURES_COLUMN,
                        DataTypes.createArrayType(DataTypes.StringType))
                .add("failure_count", DataTypes.IntegerType)
                .add("quarantined_at", DataTypes.TimestampType);
        spark.createDataFrame(List.of(), empty).createOrReplaceTempView("silver_quarantine");
    }

    /** Name to SQL, in the order a reader would want to see them. */
    public static Map<String, String> catalog() {
        Map<String, String> q = new LinkedHashMap<>();

        q.put("Busiest minutes by session creation", """
                SELECT window_start, game_id, region,
                       sessions_created, player_joins, distinct_sessions
                FROM gold_session_lifecycle_1m
                ORDER BY sessions_created DESC, window_start
                LIMIT 10
                """);

        q.put("Regional load, most recent windows", """
                SELECT window_start, region, active_sessions,
                       peak_session_size, event_count
                FROM gold_region_concurrency_1m
                ORDER BY window_start DESC, region
                LIMIT 10
                """);

        q.put("Session length distribution by game", """
                SELECT game_id, region, sessions_observed, sessions_started,
                       start_rate, p50_duration_seconds, p95_duration_seconds,
                       round(avg_peak_players, 2) AS avg_peak_players
                FROM gold_matchmaking_funnel
                ORDER BY sessions_observed DESC
                """);

        q.put("Longest running sessions", """
                SELECT session_code, game_id, region, peak_players, capacity,
                       duration_seconds, completed
                FROM gold_session_duration
                ORDER BY duration_seconds DESC
                LIMIT 10
                """);

        // Sessions that were created and never started are the matchmaking
        // failure mode worth watching: players queued, a session appeared, and
        // it never got enough of them to begin.
        q.put("Abandoned sessions, created but never started", """
                SELECT game_id, region,
                       count(*) AS abandoned,
                       round(avg(peak_players), 2) AS avg_players_reached,
                       round(avg(duration_seconds), 1) AS avg_seconds_before_giving_up
                FROM gold_session_duration
                WHERE start_events = 0
                GROUP BY game_id, region
                ORDER BY abandoned DESC
                """);

        q.put("Data quality: what is being rejected", """
                SELECT failure_reason, count(*) AS rows_rejected
                FROM (SELECT explode(quality_failures) AS failure_reason
                      FROM silver_quarantine)
                GROUP BY failure_reason
                ORDER BY rows_rejected DESC
                """);

        // Bronze holds every event, silver only the valid deduplicated ones.
        // The gap between the two counts is the pipeline's own health metric.
        q.put("Pipeline throughput: bronze in, silver out", """
                SELECT
                  (SELECT count(*) FROM bronze_session_events) AS bronze_rows,
                  (SELECT count(*) FROM silver_session_events) AS silver_rows,
                  (SELECT count(*) FROM silver_quarantine) AS quarantined_rows,
                  (SELECT count(DISTINCT event_id) FROM silver_session_events)
                    AS distinct_events_in_silver
                """);

        return q;
    }

    /** Runs every query and returns the rendered result, ready to print. */
    public List<String> runAll(SparkSession spark) {
        registerViews(spark);
        List<String> out = new ArrayList<>();
        catalog().forEach((name, sql) -> {
            StringBuilder sb = new StringBuilder();
            sb.append("== ").append(name).append(" ==\n");
            try {
                Dataset<Row> result = spark.sql(sql);
                sb.append(result.showString(20, 40, false));
            } catch (Exception ex) {
                // A table that has not been built yet is a normal state for a
                // partially run pipeline, not an error worth aborting on.
                sb.append("  (unavailable: ").append(ex.getMessage().split("\n")[0]).append(")\n");
            }
            out.add(sb.toString());
        });
        return out;
    }
}
