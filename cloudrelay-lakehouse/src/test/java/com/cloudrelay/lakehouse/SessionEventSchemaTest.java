package com.cloudrelay.lakehouse;

import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the wire format between the service and the lakehouse.
 *
 * <p>Half of a contract test. The other half is {@code AnalyticsWireFormatTest}
 * in the service module, which asserts that the JSON its serialiser actually
 * produces has exactly these field names. The two modules do not depend on each
 * other — putting Spring Boot on Spark's test classpath is a fight worth
 * avoiding — so the contract is held by two tests that share one literal list
 * instead. Renaming a field on either side fails a build on that side, which is
 * the point: the alternative is discovering it as a column of nulls in silver a
 * week later.
 */
class SessionEventSchemaTest {

    /** Must match ENVELOPE_FIELDS in the service module's AnalyticsWireFormatTest. */
    static final List<String> ENVELOPE_FIELDS =
            List.of("eventId", "eventTime", "type", "sessionCode", "schemaVersion", "session");

    /** Must match SESSION_FIELDS in the service module's AnalyticsWireFormatTest. */
    static final List<String> SESSION_FIELDS = List.of(
            "id", "sessionCode", "gameId", "region", "state", "host", "players",
            "maxPlayers", "minPlayersToStart", "createdAt", "updatedAt", "expiresAt",
            "currentPlayerCount", "isFull", "uptimeSeconds");

    @Test
    void envelopeMatchesTheAgreedFields() {
        assertThat(SessionEventSchema.envelope().fieldNames())
                .containsExactlyElementsOf(ENVELOPE_FIELDS);
    }

    @Test
    void sessionPayloadMatchesTheAgreedFields() {
        StructType session = (StructType) SessionEventSchema.envelope()
                .apply("session").dataType();
        assertThat(session.fieldNames()).containsExactlyElementsOf(SESSION_FIELDS);
    }

    /**
     * The producer sends {@code session.metadata}, a free-form per-game map, and
     * this schema does not declare it.
     *
     * <p>That is deliberate and it is safe: Spark's {@code from_json} ignores
     * fields the schema does not mention rather than failing on them. Typing an
     * arbitrary map would mean either forcing every game into one shape or
     * widening the column until it carried no information. The data is not lost
     * — bronze holds the whole payload — it is simply not silver's to type.
     */
    @Test
    void freeFormMetadataIsLeftInBronzeOnPurpose() {
        StructType session = (StructType) SessionEventSchema.envelope()
                .apply("session").dataType();
        assertThat(session.fieldNames()).doesNotContain("metadata");
    }

    @Test
    void timestampsArriveAsStringsAndAreCastInSilver() {
        // Declared as strings on purpose. Letting Spark parse them during
        // from_json would fail the whole envelope on one bad timestamp; cast
        // afterwards, a bad timestamp becomes one null that the
        // event_time_parses rule quarantines on its own.
        assertThat(SessionEventSchema.envelope().apply("eventTime").dataType().typeName())
                .isEqualTo("string");
    }

    @Test
    void schemaVersionIsRecordedSoConsumersCanTellTheyAreBehind() {
        assertThat(SessionEventSchema.CURRENT_SCHEMA_VERSION).isEqualTo(1);
        assertThat(SessionEventSchema.envelope().apply("schemaVersion").dataType().typeName())
                .isEqualTo("integer");
    }
}
