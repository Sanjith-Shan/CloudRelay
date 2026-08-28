package com.cloudrelay.matchmaking;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * The queue of waiting players, indexed for the two questions the matchmaker
 * asks.
 *
 * <h2>Why two orderings and not one</h2>
 *
 * <p>Forming a party needs both "who has waited longest" — because fairness
 * means the queue is served in order — and "who is near this rating" — because
 * that is what makes the match worth playing. Those are different sort keys
 * over the same set, and no single ordering answers both. A queue sorted by
 * arrival time answers the first in constant time and forces a full scan for
 * the second; sorted by rating, the reverse.
 *
 * <p>So the implementations keep both orderings, and pay an extra O(log n) on
 * insert and removal to keep them consistent. That is the trade: writes get
 * slightly more expensive so that the read on the hot path stops being linear.
 * It is the right way round, because a player enqueues once and the matchmaker
 * runs against the queue continuously.
 *
 * <p>Both production implementations are skip lists underneath — a Redis sorted
 * set is one, and so is {@link java.util.concurrent.ConcurrentSkipListMap} —
 * so the complexity below holds either way.
 *
 * <table>
 *   <caption>Cost per operation, n players queued for one game and region</caption>
 *   <tr><th>Operation</th><th>Cost</th></tr>
 *   <tr><td>{@link #add}</td><td>O(log n)</td></tr>
 *   <tr><td>{@link #longestWaiting}</td><td>O(log n)</td></tr>
 *   <tr><td>{@link #withinRating}</td><td>O(log n + k), k matches returned</td></tr>
 *   <tr><td>{@link #removeAll}</td><td>O(m log n), m removed</td></tr>
 * </table>
 */
public interface WaitingPlayerIndex {

    void add(String gameId, String region, WaitingPlayer player);

    /** The player at the head of the queue by arrival time, if any. */
    Optional<WaitingPlayer> longestWaiting(String gameId, String region);

    /**
     * Players whose rating falls in {@code [minRating, maxRating]}, nearest the
     * middle of the band first, capped at {@code limit}.
     *
     * <p>The cap matters. A wide window late in a player's wait can cover most
     * of the queue, and the matchmaker only ever needs a party's worth of
     * candidates. Without it the "expanding" part of the window would make the
     * search progressively more expensive exactly when the queue is most
     * congested.
     */
    List<WaitingPlayer> withinRating(String gameId, String region,
                                     int minRating, int maxRating, int limit);

    void removeAll(String gameId, String region, Collection<WaitingPlayer> players);

    long size(String gameId, String region);
}
