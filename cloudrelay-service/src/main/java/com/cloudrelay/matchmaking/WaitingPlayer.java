package com.cloudrelay.matchmaking;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;

/**
 * A player sitting in the matchmaking queue.
 *
 * @param rating skill rating on an Elo-like scale. 1200 is the assumed rating
 *               of a player who has never been rated, which is the usual
 *               starting point and keeps new players in the middle of the
 *               distribution rather than at the bottom of it.
 */
public record WaitingPlayer(
        String playerId,
        String displayName,
        String region,
        int rating,
        Instant enqueuedAt) implements Serializable {

    public static final int DEFAULT_RATING = 1200;

    public Duration waitedBy(Instant now) {
        return Duration.between(enqueuedAt, now);
    }
}
