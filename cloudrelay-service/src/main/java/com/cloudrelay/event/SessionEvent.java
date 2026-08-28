package com.cloudrelay.event;

import com.cloudrelay.dto.SessionResponse;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Session lifecycle event published on the Redis channel so that every
 * service replica can relay the update to its own WebSocket subscribers.
 * A client connected to replica A still receives events for actions that
 * were handled by replica B.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SessionEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    private String type;
    private String sessionCode;
    private SessionResponse session;
}
