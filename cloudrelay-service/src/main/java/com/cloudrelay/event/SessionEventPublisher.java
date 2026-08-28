package com.cloudrelay.event;

import com.cloudrelay.analytics.AnalyticsEventSink;
import com.cloudrelay.dto.SessionResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

/**
 * Publishes session lifecycle events to two places with two different jobs.
 *
 * <p><b>Redis pub/sub</b> fans the event out to every running replica, which
 * keeps WebSocket clients in sync no matter which instance handled the request.
 * This path is load bearing: a client that misses an event here sees a stale
 * lobby.
 *
 * <p><b>Kafka</b> carries the same event to the lakehouse for storage and
 * analysis. This path is not load bearing and must never behave as if it were.
 *
 * <p>Both are wrapped so that a failure in either one cannot reach the caller.
 * The caller has already committed a session change to MongoDB; failing the
 * request now would report an error for an operation that actually succeeded,
 * which is a worse outcome than a missing UI refresh or a gap in a dashboard.
 * The two are also independent of each other — Redis being down must not stop
 * the event reaching Kafka, and vice versa — so they get separate try blocks
 * rather than one around both.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SessionEventPublisher {

    public static final String CHANNEL = "cloudrelay:session-events";

    private final RedisTemplate<String, Object> redisTemplate;
    private final AnalyticsEventSink analyticsSink;

    public void publish(String type, SessionResponse session) {
        SessionEvent event = SessionEvent.builder()
                // Assigned here, once, rather than by either consumer. This is
                // the identity a retry keeps and the lakehouse deduplicates on,
                // so it has to be minted at the point the event happened.
                .eventId(UUID.randomUUID().toString())
                .eventTime(Instant.now().truncatedTo(ChronoUnit.MILLIS))
                .type(type)
                .sessionCode(session.getSessionCode())
                .schemaVersion(SessionEvent.SCHEMA_VERSION)
                .session(session)
                .build();

        publishToReplicas(event);
        teeToAnalytics(event);
    }

    private void publishToReplicas(SessionEvent event) {
        try {
            redisTemplate.convertAndSend(CHANNEL, event);
            log.debug("Published {} event for session {}",
                    event.getType(), event.getSessionCode());
        } catch (Exception ex) {
            log.warn("Failed to publish session event for {}", event.getSessionCode(), ex);
        }
    }

    private void teeToAnalytics(SessionEvent event) {
        try {
            analyticsSink.accept(event);
        } catch (Exception ex) {
            // The sink contract says it does not throw. If one ever does, that
            // is a bug in the sink, and it still does not get to fail a player's
            // request.
            log.warn("Analytics sink threw for session {}", event.getSessionCode(), ex);
        }
    }
}
