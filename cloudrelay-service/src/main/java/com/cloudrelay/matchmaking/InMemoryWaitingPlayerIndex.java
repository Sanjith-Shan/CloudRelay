package com.cloudrelay.matchmaking;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Skip-list backed index, for a single instance.
 *
 * <p>Same asymptotics as the Redis implementation and none of the network, so it
 * is what the correctness tests and the benchmark run against. It is also a
 * legitimate deployment for a single-replica service, which is why it lives in
 * main rather than in test.
 *
 * <p>Each (game, region) gets its own pair of sets. Sharding the queue this way
 * is not only about lock contention: it is what keeps every range query scoped
 * to players who could actually be matched together, so n in the complexity
 * table is the size of one game's queue in one region rather than the size of
 * the platform.
 */
public final class InMemoryWaitingPlayerIndex implements WaitingPlayerIndex {

    /** Arrival order. Ties broken by player id so the set is a total order. */
    private static final Comparator<WaitingPlayer> BY_TIME =
            Comparator.comparing(WaitingPlayer::enqueuedAt)
                    .thenComparing(WaitingPlayer::playerId);

    /** Rating order, same tiebreak, so the two sets hold identical elements. */
    private static final Comparator<WaitingPlayer> BY_RATING =
            Comparator.comparingInt(WaitingPlayer::rating)
                    .thenComparing(WaitingPlayer::playerId);

    private record Queue(NavigableSet<WaitingPlayer> byTime, NavigableSet<WaitingPlayer> byRating) {
        static Queue create() {
            return new Queue(new ConcurrentSkipListSet<>(BY_TIME),
                    new ConcurrentSkipListSet<>(BY_RATING));
        }
    }

    private final Map<String, Queue> queues = new ConcurrentHashMap<>();

    private Queue queue(String gameId, String region) {
        return queues.computeIfAbsent(gameId + ":" + region, k -> Queue.create());
    }

    @Override
    public void add(String gameId, String region, WaitingPlayer player) {
        Queue q = queue(gameId, region);
        q.byTime().add(player);
        q.byRating().add(player);
    }

    @Override
    public Optional<WaitingPlayer> longestWaiting(String gameId, String region) {
        NavigableSet<WaitingPlayer> byTime = queue(gameId, region).byTime();
        return byTime.isEmpty() ? Optional.empty() : Optional.of(byTime.first());
    }

    /**
     * Walks outward from the middle of the band, taking whichever neighbour is
     * closer, and stops as soon as {@code limit} players are collected.
     *
     * <p>The obvious implementation — collect the whole band, sort by distance,
     * take the first few — is O(k log k) in the size of the band, and the band
     * is widest exactly when the queue is most congested. Merging two iterators
     * outward from the midpoint is the same idea as the merge step of a
     * mergesort and costs O(log n) for the seek plus O(limit) for the walk,
     * independent of how many players the window happens to cover.
     */
    @Override
    public List<WaitingPlayer> withinRating(String gameId, String region,
                                            int minRating, int maxRating, int limit) {
        NavigableSet<WaitingPlayer> byRating = queue(gameId, region).byRating();
        int midpoint = minRating + (maxRating - minRating) / 2;
        // The sentinel's empty id sorts before every real id at the same
        // rating, so this splits the set cleanly with equal ratings on the
        // upper side.
        WaitingPlayer pivot = sentinel(midpoint, "");

        Iterator<WaitingPlayer> below = byRating.headSet(pivot, false).descendingIterator();
        Iterator<WaitingPlayer> above = byRating.tailSet(pivot, true).iterator();

        List<WaitingPlayer> out = new ArrayList<>(Math.min(limit, 16));
        WaitingPlayer lo = below.hasNext() ? below.next() : null;
        WaitingPlayer hi = above.hasNext() ? above.next() : null;

        while (out.size() < limit && (lo != null || hi != null)) {
            // Each iterator walks away from the midpoint, so the first player
            // outside the band on a side is the last one worth looking at there.
            if (lo != null && lo.rating() < minRating) {
                lo = null;
            }
            if (hi != null && hi.rating() > maxRating) {
                hi = null;
            }
            if (lo == null && hi == null) {
                break;
            }

            boolean takeLo = hi == null
                    || (lo != null && (midpoint - lo.rating()) <= (hi.rating() - midpoint));
            if (takeLo) {
                out.add(lo);
                lo = below.hasNext() ? below.next() : null;
            } else {
                out.add(hi);
                hi = above.hasNext() ? above.next() : null;
            }
        }
        return out;
    }

    /** A boundary key for range queries. Never inserted, only compared against. */
    private static WaitingPlayer sentinel(int rating, String idBound) {
        return new WaitingPlayer(idBound, "", "", rating, java.time.Instant.EPOCH);
    }

    @Override
    public void removeAll(String gameId, String region, Collection<WaitingPlayer> players) {
        Queue q = queue(gameId, region);
        for (WaitingPlayer p : players) {
            q.byTime().remove(p);
            q.byRating().remove(p);
        }
    }

    @Override
    public long size(String gameId, String region) {
        return queue(gameId, region).byTime().size();
    }
}
