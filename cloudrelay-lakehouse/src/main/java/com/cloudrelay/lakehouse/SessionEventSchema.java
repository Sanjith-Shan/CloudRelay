package com.cloudrelay.lakehouse;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * The contract between the service and the lakehouse, in one place.
 *
 * <p>Silver parses the raw JSON against {@link #envelope()} explicitly rather
 * than letting Spark infer a schema. Inference reads a sample of the data, so
 * it is not a contract at all: the day a field arrives null in every sampled
 * row, the column silently becomes a string and every downstream job changes
 * shape. A declared schema turns that into a parse failure that lands in the
 * quarantine table where somebody will see it.
 */
public final class SessionEventSchema {

    private SessionEventSchema() {
    }

    /** Current version the service emits. Bumped when a field is added. */
    public static final int CURRENT_SCHEMA_VERSION = 1;

    private static final StructType PLAYER = new StructType()
            .add("playerId", DataTypes.StringType)
            .add("displayName", DataTypes.StringType)
            .add("region", DataTypes.StringType)
            .add("joinedAt", DataTypes.StringType)
            .add("connected", DataTypes.BooleanType);

    private static final StructType SESSION = new StructType()
            .add("id", DataTypes.StringType)
            .add("sessionCode", DataTypes.StringType)
            .add("gameId", DataTypes.StringType)
            .add("region", DataTypes.StringType)
            .add("state", DataTypes.StringType)
            .add("host", PLAYER)
            .add("players", DataTypes.createArrayType(PLAYER))
            .add("maxPlayers", DataTypes.IntegerType)
            .add("minPlayersToStart", DataTypes.IntegerType)
            .add("createdAt", DataTypes.StringType)
            .add("updatedAt", DataTypes.StringType)
            .add("expiresAt", DataTypes.StringType)
            .add("currentPlayerCount", DataTypes.IntegerType)
            .add("isFull", DataTypes.BooleanType)
            .add("uptimeSeconds", DataTypes.LongType);

    /**
     * The JSON envelope {@code SessionEventPublisher} writes to Kafka.
     * Timestamps arrive as ISO-8601 strings and are cast in silver, because a
     * malformed timestamp should quarantine one row rather than fail the parse
     * of the whole envelope.
     */
    public static StructType envelope() {
        return new StructType()
                .add("eventId", DataTypes.StringType)
                .add("eventTime", DataTypes.StringType)
                .add("type", DataTypes.StringType)
                .add("sessionCode", DataTypes.StringType)
                .add("schemaVersion", DataTypes.IntegerType)
                .add("session", SESSION);
    }
}
