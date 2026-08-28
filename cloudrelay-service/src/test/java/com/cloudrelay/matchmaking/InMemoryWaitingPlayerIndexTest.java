package com.cloudrelay.matchmaking;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checks the skip-list index against {@link LinearScanWaitingPlayerIndex}, which
 * is the same thing written the slow obvious way.
 *
 * <p>The comparison is on the properties that are actually part of the
 * contract — which players come back, and that they come back nearest first —
 * and not on the exact sequence. The order among players sitting the same
 * distance either side of the midpoint is arbitrary, the two implementations
 * pick differently, and freezing one of those choices into a test would be
 * asserting an implementation detail rather than a requirement.
 */
class InMemoryWaitingPlayerIndexTest {

    private static final String GAME = "cyberpunk-2077";
    private static final String REGION = "us-west-2";

    private WaitingPlayer player(String id, int rating, int secondsAgo) {
        return new WaitingPlayer(id, "Player " + id, REGION, rating,
                Instant.parse("2026-08-27T12:00:00Z").minusSeconds(secondsAgo));
    }

    @Test
    void longestWaitingIsTheOldestEntry() {
        InMemoryWaitingPlayerIndex index = new InMemoryWaitingPlayerIndex();
        index.add(GAME, REGION, player("new", 1200, 1));
        index.add(GAME, REGION, player("old", 1900, 90));
        index.add(GAME, REGION, player("mid", 1500, 30));

        assertThat(index.longestWaiting(GAME, REGION))
                .get()
                .extracting(WaitingPlayer::playerId)
                .isEqualTo("old");
    }

    @Test
    void emptyQueueHasNoLongestWaiter() {
        assertThat(new InMemoryWaitingPlayerIndex().longestWaiting(GAME, REGION)).isEmpty();
    }

    @Test
    void rangeQueryExcludesPlayersOutsideTheBand() {
        InMemoryWaitingPlayerIndex index = new InMemoryWaitingPlayerIndex();
        index.add(GAME, REGION, player("low", 900, 10));
        index.add(GAME, REGION, player("in-a", 1180, 10));
        index.add(GAME, REGION, player("in-b", 1220, 10));
        index.add(GAME, REGION, player("high", 2000, 10));

        List<WaitingPlayer> found = index.withinRating(GAME, REGION, 1100, 1300, 10);

        assertThat(found).extracting(WaitingPlayer::playerId)
                .containsExactlyInAnyOrder("in-a", "in-b");
    }

    @Test
    void rangeQueryReturnsNearestFirst() {
        InMemoryWaitingPlayerIndex index = new InMemoryWaitingPlayerIndex();
        index.add(GAME, REGION, player("far-low", 1010, 10));
        index.add(GAME, REGION, player("near", 1195, 10));
        index.add(GAME, REGION, player("far-high", 1390, 10));

        List<WaitingPlayer> found = index.withinRating(GAME, REGION, 1000, 1400, 10);

        assertThat(found.get(0).playerId()).isEqualTo("near");
        assertThat(distancesFrom(found, 1200)).isSorted();
    }

    @Test
    void limitCapsTheResultAtTheNearestPlayers() {
        InMemoryWaitingPlayerIndex index = new InMemoryWaitingPlayerIndex();
        for (int i = 0; i < 50; i++) {
            index.add(GAME, REGION, player("p" + i, 1000 + i * 10, 10));
        }

        List<WaitingPlayer> found = index.withinRating(GAME, REGION, 1000, 1490, 3);

        assertThat(found).hasSize(3);
        assertThat(distancesFrom(found, 1245)).allMatch(d -> d <= 15);
    }

    @Test
    void removedPlayersLeaveBothOrderings() {
        InMemoryWaitingPlayerIndex index = new InMemoryWaitingPlayerIndex();
        WaitingPlayer gone = player("gone", 1200, 60);
        index.add(GAME, REGION, gone);
        index.add(GAME, REGION, player("stays", 1210, 10));

        index.removeAll(GAME, REGION, List.of(gone));

        assertThat(index.size(GAME, REGION)).isEqualTo(1);
        assertThat(index.longestWaiting(GAME, REGION))
                .get().extracting(WaitingPlayer::playerId).isEqualTo("stays");
        assertThat(index.withinRating(GAME, REGION, 1000, 1400, 10))
                .extracting(WaitingPlayer::playerId).containsExactly("stays");
    }

    @Test
    void queuesForDifferentGamesAndRegionsDoNotSeeEachOther() {
        InMemoryWaitingPlayerIndex index = new InMemoryWaitingPlayerIndex();
        index.add(GAME, REGION, player("west", 1200, 10));
        index.add(GAME, "eu-central-1", player("east", 1200, 60));
        index.add("fortnite", REGION, player("other-game", 1200, 90));

        assertThat(index.size(GAME, REGION)).isEqualTo(1);
        assertThat(index.longestWaiting(GAME, REGION))
                .get().extracting(WaitingPlayer::playerId).isEqualTo("west");
    }

    @Test
    void agreesWithTheLinearScanOnRandomQueues() {
        Random random = new Random(20260827L);

        for (int trial = 0; trial < 60; trial++) {
            InMemoryWaitingPlayerIndex indexed = new InMemoryWaitingPlayerIndex();
            LinearScanWaitingPlayerIndex linear = new LinearScanWaitingPlayerIndex();

            int size = 1 + random.nextInt(120);
            for (int i = 0; i < size; i++) {
                // Ratings collide on purpose: duplicate scores are where a
                // comparator based skip list is easiest to get wrong.
                WaitingPlayer p = player("p" + i, 800 + random.nextInt(30) * 25,
                        random.nextInt(300));
                indexed.add(GAME, REGION, p);
                linear.add(GAME, REGION, p);
            }

            assertThat(indexed.size(GAME, REGION)).isEqualTo(linear.size(GAME, REGION));
            assertThat(indexed.longestWaiting(GAME, REGION))
                    .isEqualTo(linear.longestWaiting(GAME, REGION));

            int centre = 800 + random.nextInt(750);
            int width = 25 + random.nextInt(400);
            int limit = 1 + random.nextInt(8);

            List<WaitingPlayer> fromIndex =
                    indexed.withinRating(GAME, REGION, centre - width, centre + width, limit);
            List<WaitingPlayer> fromScan =
                    linear.withinRating(GAME, REGION, centre - width, centre + width, limit);

            assertThat(fromIndex).hasSameSizeAs(fromScan);
            // Same distances from the midpoint, in the same order: identical
            // match quality, whichever player was picked from a tie.
            assertThat(distancesFrom(fromIndex, centre))
                    .isEqualTo(distancesFrom(fromScan, centre));
            assertThat(fromIndex).allMatch(
                    p -> p.rating() >= centre - width && p.rating() <= centre + width);
        }
    }

    private static List<Integer> distancesFrom(List<WaitingPlayer> players, int centre) {
        return players.stream().map(p -> Math.abs(p.rating() - centre)).toList();
    }
}
