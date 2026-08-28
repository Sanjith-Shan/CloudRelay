package com.cloudrelay.event;

import com.cloudrelay.dto.SessionResponse;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.Instant;

/**
 * Session lifecycle event. It has two consumers with different needs.
 *
 * <p>Redis pub/sub fans it out to every service replica so each one can relay
 * the update to its own WebSocket subscribers: a client connected to replica A
 * still receives events for actions handled by replica B.
 *
 * <p>Kafka carries the same event to the lakehouse, where it is stored and
 * replayed. That second consumer is why this class carries {@code eventId},
 * {@code eventTime} and {@code schemaVersion}, none of which the WebSocket path
 * looks at:
 *
 * <ul>
 *   <li>{@code eventId} is the deduplication identity. A producer retry after a
 *       broker timeout sends the same event twice with the same id, so the
 *       pipeline can tell a retry apart from two players genuinely doing the
 *       same thing a millisecond apart.
 *   <li>{@code eventTime} is when the event happened here, not when the
 *       pipeline saw it. Every window in the lakehouse is computed against this
 *       and would otherwise be measuring ingestion lag.
 *   <li>{@code schemaVersion} lets a consumer that predates a field recognise
 *       that it is behind, rather than reading the missing field as null.
 * </ul>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SessionEvent implements Serializable {

    private static final long serialVersionUID = 2L;

    /** Bumped whenever a field is added to this envelope or to the session payload. */
    public static final int SCHEMA_VERSION = 1;

    private String eventId;
    private Instant eventTime;
    private String type;
    private String sessionCode;

    @Builder.Default
    private int schemaVersion = SCHEMA_VERSION;

    private SessionResponse session;
}
