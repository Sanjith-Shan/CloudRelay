package com.cloudrelay.dto;

import com.cloudrelay.model.GameSession;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.cloudrelay.model.Player;
import com.cloudrelay.model.SessionState;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SessionResponse implements Serializable {

    private static final long serialVersionUID = 1L;

    private String id;
    private String sessionCode;
    private String gameId;
    private String region;
    private SessionState state;
    private Player host;
    private List<Player> players;
    private int maxPlayers;
    private int minPlayersToStart;
    private Instant createdAt;
    private Instant updatedAt;
    private Instant expiresAt;
    private Map<String, Object> metadata;
    private int currentPlayerCount;

    @JsonProperty("isFull")
    private boolean isFull;
    private long uptimeSeconds;

    public static SessionResponse from(GameSession session) {
        int playerCount = session.getPlayers() == null ? 0 : session.getPlayers().size();
        long uptime = session.getCreatedAt() == null
                ? 0
                : Duration.between(session.getCreatedAt(), Instant.now()).getSeconds();
        return SessionResponse.builder()
                .id(session.getId())
                .sessionCode(session.getSessionCode())
                .gameId(session.getGameId())
                .region(session.getRegion())
                .state(session.getState())
                .host(session.getHost())
                .players(session.getPlayers())
                .maxPlayers(session.getMaxPlayers())
                .minPlayersToStart(session.getMinPlayersToStart())
                .createdAt(session.getCreatedAt())
                .updatedAt(session.getUpdatedAt())
                .expiresAt(session.getExpiresAt())
                .metadata(session.getMetadata())
                .currentPlayerCount(playerCount)
                .isFull(playerCount >= session.getMaxPlayers())
                .uptimeSeconds(uptime)
                .build();
    }
}
