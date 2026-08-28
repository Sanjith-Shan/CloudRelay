package com.cloudrelay.matchmaking;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The obvious implementation: keep a list, scan it.
 *
 * <p>This is the baseline the indexed version is measured against, and it is
 * also the oracle it is checked against. It is deliberately the straightforward
 * thing somebody would write first — a list, a full scan for the oldest entry,
 * a filter and a sort for the rating band — and it is correct. Being correct is
 * the point: if the skip list index ever disagrees with it,
 * {@code InMemoryWaitingPlayerIndexTest} fails, and the failure is in the
 * clever one.
 *
 * <p>Costs, against n queued players:
 * add O(1), longestWaiting O(n), withinRating O(n log n), removeAll O(n·m).
 */
final class LinearScanWaitingPlayerIndex implements WaitingPlayerIndex {

    private final Map<String, List<WaitingPlayer>> queues = new HashMap<>();

    private List<WaitingPlayer> queue(String gameId, String region) {
        return queues.computeIfAbsent(gameId + ":" + region, k -> new ArrayList<>());
    }

    @Override
    public void add(String gameId, String region, WaitingPlayer player) {
        queue(gameId, region).add(player);
    }

    @Override
    public Optional<WaitingPlayer> longestWaiting(String gameId, String region) {
        return queue(gameId, region).stream()
                .min(Comparator.comparing(WaitingPlayer::enqueuedAt)
                        .thenComparing(WaitingPlayer::playerId));
    }

    @Override
    public List<WaitingPlayer> withinRating(String gameId, String region,
                                            int minRating, int maxRating, int limit) {
        int midpoint = minRating + (maxRating - minRating) / 2;
        List<WaitingPlayer> matches = new ArrayList<>();
        for (WaitingPlayer p : queue(gameId, region)) {
            if (p.rating() >= minRating && p.rating() <= maxRating) {
                matches.add(p);
            }
        }
        matches.sort(Comparator
                .comparingInt((WaitingPlayer p) -> Math.abs(p.rating() - midpoint))
                // The skip list walks the lower side first at equal distance,
                // so the oracle has to break ties the same way or the two
                // disagree on ordering while agreeing on membership.
                .thenComparingInt(WaitingPlayer::rating)
                .thenComparing(WaitingPlayer::playerId));
        return matches.size() > limit ? new ArrayList<>(matches.subList(0, limit)) : matches;
    }

    @Override
    public void removeAll(String gameId, String region, Collection<WaitingPlayer> players) {
        queue(gameId, region).removeAll(players);
    }

    @Override
    public long size(String gameId, String region) {
        return queue(gameId, region).size();
    }
}
