package com.cloudrelay.matchmaking;

import java.time.Duration;

/**
 * How wide a skill range a waiting player will accept, as a function of how
 * long they have been waiting.
 *
 * <h2>The trade this encodes</h2>
 *
 * <p>Matchmaking has two goals that pull against each other. A narrow skill
 * window produces even games and long queues; a wide one fills lobbies
 * instantly and produces matches nobody enjoys. Neither setting is right on its
 * own, because the correct answer depends on something that is not known when
 * the player joins the queue: how many other players are available near their
 * rating right now.
 *
 * <p>Widening over time resolves that without having to know it. A player in a
 * dense part of the rating distribution matches almost immediately inside the
 * narrow initial window and gets an even game. A player at the far tail, where
 * nobody comparable is queued, is not held forever — their window grows until
 * it reaches somebody. The system discovers the local density of the queue by
 * waiting, which is cheaper and more robust than trying to model it.
 *
 * <p>Growth is linear rather than exponential. Exponential widening spends most
 * of its time either too narrow to help or so wide the constraint is gone, and
 * the point at which it flips is sensitive to the rate constant. Linear
 * widening is predictable: a player can be told "you will be matched within
 * {@code (maxWidth - initialWidth) / growthPerSecond} seconds or the game is
 * short of players", and that statement stays true.
 *
 * <p>All three parameters are policy, not physics. They belong to whoever tunes
 * the game, which is why this is a value object and not a constant.
 */
public record ExpandingWindow(int initialWidth, int growthPerSecond, int maxWidth) {

    /**
     * Defaults chosen for a session game: 100 rating points is roughly an even
     * match, 25 points per second reaches the 600 point cap in 20 seconds, and
     * 600 points is wide enough that anybody queued at all is a candidate.
     */
    public static ExpandingWindow defaults() {
        return new ExpandingWindow(100, 25, 600);
    }

    public ExpandingWindow {
        if (initialWidth < 0 || growthPerSecond < 0 || maxWidth < initialWidth) {
            throw new IllegalArgumentException(
                    "window must be non-negative and maxWidth must not be below initialWidth");
        }
    }

    /** Half-width of the acceptable rating band after waiting {@code waited}. */
    public int widthAfter(Duration waited) {
        long seconds = Math.max(0, waited.getSeconds());
        long width = initialWidth + growthPerSecond * seconds;
        return (int) Math.min(width, maxWidth);
    }

    /** How long until a player's window reaches its maximum. */
    public Duration timeToMaxWidth() {
        if (growthPerSecond == 0) {
            return Duration.ofSeconds(Long.MAX_VALUE);
        }
        return Duration.ofSeconds((maxWidth - initialWidth) / growthPerSecond);
    }
}
