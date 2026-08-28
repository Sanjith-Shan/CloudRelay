package com.cloudrelay.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.Instant;

/**
 * A player participating in a game session. Embedded inside GameSession
 * documents rather than stored as a top level collection.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Player implements Serializable {

    private static final long serialVersionUID = 1L;

    private String playerId;
    private String displayName;
    private String region;
    private Instant joinedAt;

    @Builder.Default
    private boolean connected = true;
}
