package com.cloudrelay.event;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

/**
 * Listens on the Redis session events channel and relays each event to the
 * local STOMP broker so WebSocket clients subscribed to
 * /topic/session/{sessionCode} receive real time updates.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SessionEventSubscriber implements MessageListener {

    private final SimpMessagingTemplate messagingTemplate;
    private final RedisSerializer<Object> eventSerializer;

    @Override
    public void onMessage(Message message, byte[] pattern) {
        try {
            Object payload = eventSerializer.deserialize(message.getBody());
            if (payload instanceof SessionEvent event) {
                messagingTemplate.convertAndSend(
                        "/topic/session/" + event.getSessionCode(), event);
                log.debug("Relayed {} event for session {}", event.getType(), event.getSessionCode());
            }
        } catch (Exception ex) {
            log.warn("Failed to relay session event", ex);
        }
    }
}
