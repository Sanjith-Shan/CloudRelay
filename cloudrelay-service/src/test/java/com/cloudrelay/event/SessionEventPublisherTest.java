package com.cloudrelay.event;

import com.cloudrelay.analytics.AnalyticsEventSink;
import com.cloudrelay.dto.SessionResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.RedisTemplate;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class SessionEventPublisherTest {

    @Mock
    private RedisTemplate<String, Object> redisTemplate;

    @Mock
    private AnalyticsEventSink analyticsSink;

    private SessionEventPublisher publisher;

    @BeforeEach
    void setUp() {
        publisher = new SessionEventPublisher(redisTemplate, analyticsSink);
    }

    private SessionResponse session() {
        return SessionResponse.builder()
                .sessionCode("ABCD1234")
                .gameId("cyberpunk-2077")
                .region("us-west-2")
                .build();
    }

    @Test
    void stampsIdentityAndTimeOnEveryEvent() {
        publisher.publish("SESSION_CREATED", session());

        ArgumentCaptor<SessionEvent> captor = ArgumentCaptor.forClass(SessionEvent.class);
        verify(analyticsSink).accept(captor.capture());
        SessionEvent event = captor.getValue();

        assertThat(event.getEventId()).isNotBlank();
        assertThat(event.getEventTime()).isNotNull();
        assertThat(event.getType()).isEqualTo("SESSION_CREATED");
        assertThat(event.getSessionCode()).isEqualTo("ABCD1234");
        assertThat(event.getSchemaVersion()).isEqualTo(SessionEvent.SCHEMA_VERSION);
    }

    @Test
    void truncatesEventTimeToMilliseconds() {
        // Spark's string-to-timestamp cast accepts at most six fractional
        // digits, and Instant.now() carries up to nine on Linux. Truncating
        // here is what stops every event quarantining on a Linux host.
        publisher.publish("SESSION_CREATED", session());

        ArgumentCaptor<SessionEvent> captor = ArgumentCaptor.forClass(SessionEvent.class);
        verify(analyticsSink).accept(captor.capture());
        Instant eventTime = captor.getValue().getEventTime();

        assertThat(eventTime).isEqualTo(eventTime.truncatedTo(ChronoUnit.MILLIS));
    }

    @Test
    void sendsTheSameEventToBothConsumers() {
        publisher.publish("PLAYER_JOINED", session());

        ArgumentCaptor<SessionEvent> toRedis = ArgumentCaptor.forClass(SessionEvent.class);
        verify(redisTemplate).convertAndSend(eq(SessionEventPublisher.CHANNEL),
                toRedis.capture());
        ArgumentCaptor<SessionEvent> toKafka = ArgumentCaptor.forClass(SessionEvent.class);
        verify(analyticsSink).accept(toKafka.capture());

        assertThat(toRedis.getValue().getEventId())
                .isEqualTo(toKafka.getValue().getEventId());
    }

    @Test
    void eventIdsAreUniquePerEvent() {
        List<String> ids = new ArrayList<>();
        ArgumentCaptor<SessionEvent> captor = ArgumentCaptor.forClass(SessionEvent.class);
        for (int i = 0; i < 100; i++) {
            publisher.publish("PLAYER_JOINED", session());
        }
        verify(analyticsSink, org.mockito.Mockito.times(100)).accept(captor.capture());
        captor.getAllValues().forEach(e -> ids.add(e.getEventId()));

        assertThat(ids).doesNotHaveDuplicates();
    }

    @Test
    void aRedisFailureDoesNotStopTheEventReachingKafka() {
        doThrow(new RuntimeException("redis down"))
                .when(redisTemplate).convertAndSend(any(), any());

        assertThatCode(() -> publisher.publish("SESSION_CREATED", session()))
                .doesNotThrowAnyException();

        verify(analyticsSink).accept(any(SessionEvent.class));
    }

    @Test
    void aSinkFailureDoesNotStopTheEventReachingReplicas() {
        doThrow(new RuntimeException("sink exploded"))
                .when(analyticsSink).accept(any());

        assertThatCode(() -> publisher.publish("SESSION_CREATED", session()))
                .doesNotThrowAnyException();

        verify(redisTemplate).convertAndSend(eq(SessionEventPublisher.CHANNEL),
                any(SessionEvent.class));
    }

    @Test
    void bothConsumersFailingStillLeavesTheCallerUnharmed() {
        // The session change is already committed to MongoDB by this point.
        // Throwing here would report an error for an operation that succeeded.
        doThrow(new RuntimeException("redis down"))
                .when(redisTemplate).convertAndSend(any(), any());
        doThrow(new RuntimeException("kafka down"))
                .when(analyticsSink).accept(any());

        assertThatCode(() -> publisher.publish("SESSION_TERMINATED", session()))
                .doesNotThrowAnyException();
    }
}
