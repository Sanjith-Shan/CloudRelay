package com.cloudrelay.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.io.Serializable;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * A cloud gaming session persisted in MongoDB. Redis holds a short lived
 * cached copy keyed by session code for low latency reads.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "sessions")
@CompoundIndex(name = "matchmaking_idx", def = "{'gameId': 1, 'region': 1, 'state': 1}")
public class GameSession implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    private String id;

    @Indexed(unique = true)
    private String sessionCode;

    private String gameId;

    private String region;

    @Builder.Default
    private SessionState state = SessionState.WAITING;

    private Player host;

    private List<Player> players;

    @Builder.Default
    private int maxPlayers = 4;

    @Builder.Default
    private int minPlayersToStart = 2;

    private Instant createdAt;

    private Instant updatedAt;

    /**
     * MongoDB removes the document automatically once this instant passes.
     * The TTL monitor uses the field value itself as the expiration time.
     */
    @Indexed(expireAfterSeconds = 0)
    private Instant expiresAt;

    private Map<String, Object> metadata;

    /**
     * Optimistic concurrency guard. Concurrent writers racing on the same
     * session document fail fast instead of silently losing player joins.
     */
    @Version
    private Long version;
}
