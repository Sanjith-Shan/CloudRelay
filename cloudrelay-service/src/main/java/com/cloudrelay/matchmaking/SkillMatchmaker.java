package com.cloudrelay.matchmaking;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Forms parties of similarly rated players, oldest waiter first.
 *
 * <h2>The algorithm</h2>
 *
 * <ol>
 *   <li>Take the player who has waited longest. They are the anchor, and
 *       serving the queue in arrival order is what stops a player at an
 *       unpopular rating from being passed over indefinitely by newer arrivals
 *       who are easier to match.</li>
 *   <li>Compute their acceptable skill band from how long they have waited
 *       ({@link ExpandingWindow}).</li>
 *   <li>Pull the nearest candidates inside that band. The band is centred on the
 *       anchor's rating, so "nearest the middle of the band" and "nearest the
 *       anchor" are the same ordering and no second sort is needed.</li>
 *   <li>If there are enough, take them. If not, form nothing and leave everyone
 *       queued — the anchor's window is wider on the next attempt, so the
 *       failure is self-correcting rather than something to retry differently.</li>
 * </ol>
 *
 * <h2>Cost</h2>
 *
 * <p>O(log n + p) per attempt for a party of p, against n players queued for
 * that game and region. The linear alternative — scan everyone, sort by rating
 * distance, take the closest — is O(n log n), and the gap between them is what
 * {@code MatchmakingBenchmarkTest} measures.
 *
 * <h2>What this deliberately does not do</h2>
 *
 * <p>It does not balance teams, only assembles a lobby. Splitting a formed party
 * into even sides is a separate problem — a partition problem, and NP-hard in
 * general — and mixing it in here would make a fast, predictable step depend on
 * a slow, approximate one.
 */
public final class SkillMatchmaker {

    /**
     * Candidates fetched per attempt, as a multiple of party size.
     *
     * <p>More than needed, because some may since have been matched by a
     * concurrent attempt, and bounded, because the whole point of the index is
     * that the work per attempt does not grow with the queue.
     */
    private static final int CANDIDATE_FACTOR = 3;

    private final WaitingPlayerIndex index;
    private final ExpandingWindow window;

    public SkillMatchmaker(WaitingPlayerIndex index, ExpandingWindow window) {
        this.index = index;
        this.window = window;
    }

    public Optional<Party> formParty(String gameId, String region, int partySize, Instant now) {
        if (partySize < 1) {
            throw new IllegalArgumentException("partySize must be at least 1");
        }
        Optional<WaitingPlayer> maybeAnchor = index.longestWaiting(gameId, region);
        if (maybeAnchor.isEmpty()) {
            return Optional.empty();
        }
        WaitingPlayer anchor = maybeAnchor.get();

        int width = window.widthAfter(anchor.waitedBy(now));
        List<WaitingPlayer> candidates = index.withinRating(
                gameId, region,
                anchor.rating() - width, anchor.rating() + width,
                partySize * CANDIDATE_FACTOR);

        List<WaitingPlayer> members = new ArrayList<>(partySize);
        members.add(anchor);
        for (WaitingPlayer candidate : candidates) {
            if (members.size() == partySize) {
                break;
            }
            if (!candidate.playerId().equals(anchor.playerId())) {
                members.add(candidate);
            }
        }

        if (members.size() < partySize) {
            // Not enough compatible players yet. Everyone stays queued and the
            // anchor's window is wider next time.
            return Optional.empty();
        }

        index.removeAll(gameId, region, members);
        return Optional.of(new Party(List.copyOf(members), anchor.waitedBy(now)));
    }

    public void enqueue(String gameId, String region, WaitingPlayer player) {
        index.add(gameId, region, player);
    }

    public long queueDepth(String gameId, String region) {
        return index.size(gameId, region);
    }

    public ExpandingWindow window() {
        return window;
    }
}
