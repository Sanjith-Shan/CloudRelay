package com.cloudrelay.lakehouse.transform;

import com.cloudrelay.lakehouse.SessionEventSchema;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;

import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.array_compact;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.size;
import static org.apache.spark.sql.functions.when;

/**
 * The rules that decide whether a parsed event is allowed into silver.
 *
 * <p>Bad rows are routed, not dropped and not fatal. Dropping them makes the
 * pipeline look healthy while it quietly loses data, and failing the batch
 * lets one malformed event stop every good one behind it. Quarantining keeps
 * the bad row, the reason it was rejected, and the bronze coordinates needed to
 * replay it once the producer is fixed.
 *
 * <p>Adding a rule is adding one entry to {@link #EXPECTATIONS}; the split, the
 * failure list and the quarantine write all follow from it.
 */
public final class DataQuality {

    private DataQuality() {
    }

    public static final String FAILURES_COLUMN = "quality_failures";

    private static final List<String> KNOWN_EVENT_TYPES = List.of(
            "SESSION_CREATED", "PLAYER_JOINED", "PLAYER_LEFT",
            "SESSION_STARTED", "SESSION_TERMINATED");

    private static final List<String> KNOWN_STATES = List.of(
            "WAITING", "STARTING", "ACTIVE", "PAUSED", "TERMINATED");

    /**
     * Evaluated against the flattened silver columns, in order.
     *
     * <p>{@code envelope_parses} is first and deliberately broad: Spark's
     * PERMISSIVE JSON parser turns an unparseable record into a struct of all
     * nulls rather than raising, so a null {@code event_id} is the signal that
     * the bytes were not valid JSON at all. Without this rule that record would
     * pass every other check vacuously and arrive in silver as an empty row.
     */
    public static final List<Expectation> EXPECTATIONS = List.of(
            new Expectation("envelope_parses", col("event_id").isNotNull()),
            new Expectation("event_time_parses", col("event_time").isNotNull()),
            new Expectation("event_type_known",
                    col("event_type").isin(KNOWN_EVENT_TYPES.toArray())),
            new Expectation("session_code_present",
                    col("session_code").isNotNull().and(col("session_code").notEqual(""))),
            new Expectation("game_id_present", col("game_id").isNotNull()),
            new Expectation("region_present", col("region").isNotNull()),
            new Expectation("session_state_known",
                    col("session_state").isin(KNOWN_STATES.toArray())),
            new Expectation("player_count_non_negative", col("player_count").geq(0)),
            new Expectation("max_players_positive", col("max_players").gt(0)),
            new Expectation("player_count_within_capacity",
                    col("player_count").leq(col("max_players"))),
            // A newer producer rolling out ahead of this job emits a version
            // this code has never seen. Quarantining is the safe direction:
            // the rows are kept and replayable, and the alert fires on the
            // deploy rather than on a silent column of nulls a week later.
            new Expectation("schema_version_supported",
                    col("schema_version").leq(lit(SessionEventSchema.CURRENT_SCHEMA_VERSION))));

    /**
     * Adds {@code quality_failures}: the names of every rule this row broke, or
     * an empty array. One pass, one column, and the same expression drives both
     * the silver filter and the quarantine reason.
     */
    public static Dataset<Row> annotate(Dataset<Row> events) {
        Column[] failures = EXPECTATIONS.stream()
                .map(e -> when(e.condition(), lit(null).cast("string")).otherwise(lit(e.name())))
                .toArray(Column[]::new);
        return events.withColumn(FAILURES_COLUMN, array_compact(array(failures)));
    }

    public static Column isValid() {
        return size(col(FAILURES_COLUMN)).equalTo(0);
    }
}
