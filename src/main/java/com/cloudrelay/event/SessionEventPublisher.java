package com.cloudrelay.event;

import com.cloudrelay.dto.SessionResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

/**
 * Publishes session lifecycle events to a Redis pub sub channel. Redis fans
 * the event out to every running replica of this service, which keeps
 * WebSocket clients in sync no matter which instance handled the request.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SessionEventPublisher {

    public static final String CHANNEL = "cloudrelay:session-events";

    private final RedisTemplate<String, Object> redisTemplate;

    public void publish(String type, SessionResponse session) {
        try {
            SessionEvent event = SessionEvent.builder()
                    .type(type)
                    .sessionCode(session.getSessionCode())
                    .session(session)
                    .build();
            redisTemplate.convertAndSend(CHANNEL, event);
            log.debug("Published {} event for session {}", type, session.getSessionCode());
        } catch (Exception ex) {
            log.warn("Failed to publish session event for {}", session.getSessionCode(), ex);
        }
    }
}
