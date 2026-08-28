package com.cloudrelay.analytics;

import com.cloudrelay.config.AnalyticsConfig;
import com.cloudrelay.dto.SessionResponse;
import com.cloudrelay.event.SessionEvent;
import com.cloudrelay.model.Player;
import com.cloudrelay.model.SessionState;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The producer half of the contract with the lakehouse.
 *
 * <p>{@code SessionEventSchemaTest} in the lakehouse module asserts the same two
 * field lists against the schema Spark parses with. The modules do not depend
 * on each other, so this pair of tests is what holds the two ends together:
 * rename a field here without renaming it there and this build fails, which is
 * a great deal better than the column quietly becoming null in silver and
 * nobody noticing until a dashboard is wrong.
 */
class AnalyticsWireFormatTest {

    /** Must match ENVELOPE_FIELDS in the lakehouse module's SessionEventSchemaTest. */
    private static final List<String> ENVELOPE_FIELDS =
            List.of("eventId", "eventTime", "type", "sessionCode", "schemaVersion", "session");

    /**
     * Fields the wire format carries that silver deliberately does not model.
     *
     * <p>{@code metadata} is a free-form per-game map. It has no stable schema
     * by design, so typing it in silver would mean either inventing one that
     * every game has to fit or widening the column until it means nothing. It
     * stays in bronze inside the raw payload, where a game-specific job can
     * parse its own shape out of it, and silver stays typed.
     *
     * <p>Listed rather than ignored so that the next field to appear here has to
     * be a decision somebody made, not something that slipped past the contract.
     */
    private static final List<String> NOT_MODELLED_IN_SILVER = List.of("metadata");

    /** Must match SESSION_FIELDS in the lakehouse module's SessionEventSchemaTest. */
    private static final List<String> SESSION_FIELDS = List.of(
            "id", "sessionCode", "gameId", "region", "state", "host", "players",
            "maxPlayers", "minPlayersToStart", "createdAt", "updatedAt", "expiresAt",
            "currentPlayerCount", "isFull", "uptimeSeconds");

    private final ObjectMapper mapper = new AnalyticsConfig().analyticsObjectMapper();

    private SessionEvent event() {
        Player host = Player.builder()
                .playerId("player-1").displayName("Alice").region("us-west-2")
                .joinedAt(Instant.parse("2026-08-27T12:00:00Z")).connected(true).build();
        List<Player> players = new ArrayList<>(List.of(host));

        return SessionEvent.builder()
                .eventId("11111111-2222-3333-4444-555555555555")
                .eventTime(Instant.parse("2026-08-27T12:00:00.123Z"))
                .type("SESSION_CREATED")
                .sessionCode("ABCD1234")
                .schemaVersion(SessionEvent.SCHEMA_VERSION)
                .session(SessionResponse.builder()
                        .id("session-id")
                        .sessionCode("ABCD1234")
                        .gameId("cyberpunk-2077")
                        .region("us-west-2")
                        .state(SessionState.WAITING)
                        .host(host)
                        .players(players)
                        .maxPlayers(4)
                        .minPlayersToStart(2)
                        .createdAt(Instant.parse("2026-08-27T12:00:00Z"))
                        .updatedAt(Instant.parse("2026-08-27T12:00:00Z"))
                        .expiresAt(Instant.parse("2026-08-27T14:00:00Z"))
                        .currentPlayerCount(1)
                        .isFull(false)
                        .uptimeSeconds(0)
                        .build())
                .build();
    }

    private JsonNode serialised() throws Exception {
        return mapper.readTree(mapper.writeValueAsString(event()));
    }

    @Test
    void envelopeCarriesExactlyTheAgreedFields() throws Exception {
        assertThat(serialised().fieldNames()).toIterable()
                .containsExactlyInAnyOrderElementsOf(ENVELOPE_FIELDS);
    }

    @Test
    void sessionPayloadCarriesExactlyTheAgreedFields() throws Exception {
        List<String> actual = new ArrayList<>();
        serialised().get("session").fieldNames().forEachRemaining(actual::add);

        List<String> expected = new ArrayList<>(SESSION_FIELDS);
        expected.addAll(NOT_MODELLED_IN_SILVER);

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    void silverModelsEveryWireFieldExceptTheOnesItDeclinesTo() throws Exception {
        List<String> actual = new ArrayList<>();
        serialised().get("session").fieldNames().forEachRemaining(actual::add);

        assertThat(actual).containsAll(SESSION_FIELDS);
        assertThat(actual).containsAll(NOT_MODELLED_IN_SILVER);
    }

    @Test
    void timestampsAreIsoStringsNotEpochNumbers() throws Exception {
        JsonNode json = serialised();
        assertThat(json.get("eventTime").isTextual()).isTrue();
        assertThat(json.get("eventTime").asText()).isEqualTo("2026-08-27T12:00:00.123Z");
        assertThat(json.get("session").get("createdAt").asText())
                .isEqualTo("2026-08-27T12:00:00Z");
    }

    @Test
    void instantsAreTruncatedToMilliseconds() throws Exception {
        // Nine fractional digits are what Instant.now() produces on Linux, and
        // Spark's string-to-timestamp cast returns null past six. Truncating at
        // the producer is what stops that becoming a quarantined event.
        SessionEvent nanos = SessionEvent.builder()
                .eventId("id")
                .eventTime(Instant.parse("2026-08-27T12:00:00.123456789Z"))
                .type("SESSION_CREATED").sessionCode("ABCD1234")
                .session(SessionResponse.builder().sessionCode("ABCD1234").build())
                .build();

        JsonNode json = mapper.readTree(mapper.writeValueAsString(nanos));

        assertThat(json.get("eventTime").asText()).isEqualTo("2026-08-27T12:00:00.123Z");
    }

    @Test
    void theStateEnumIsWrittenAsItsName() throws Exception {
        assertThat(serialised().get("session").get("state").asText()).isEqualTo("WAITING");
    }

    @Test
    void schemaVersionIsPresentAndNumeric() throws Exception {
        JsonNode version = serialised().get("schemaVersion");
        assertThat(version.isInt()).isTrue();
        assertThat(version.asInt()).isEqualTo(SessionEvent.SCHEMA_VERSION);
    }

    @Test
    void thePayloadIsFlatEnoughForSparkToParseWithoutInference() throws Exception {
        // The lakehouse declares this shape explicitly rather than inferring it.
        // Nested players are fine, arrays of arrays or free-form maps would not
        // be, so the shape is pinned here as well.
        JsonNode players = serialised().get("session").get("players");
        assertThat(players.isArray()).isTrue();
        assertThat(players.get(0).fieldNames()).toIterable()
                .containsExactlyInAnyOrder("playerId", "displayName", "region",
                        "joinedAt", "connected");
    }
}
