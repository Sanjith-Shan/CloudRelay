package com.cloudrelay.matchmaking;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SkillMatchmakerTest {

    private static final String GAME = "cyberpunk-2077";
    private static final String REGION = "us-west-2";
    private static final Instant NOW = Instant.parse("2026-08-27T12:00:00Z");

    private final InMemoryWaitingPlayerIndex index = new InMemoryWaitingPlayerIndex();
    private final SkillMatchmaker matchmaker =
            new SkillMatchmaker(index, new ExpandingWindow(100, 25, 600));

    private void queue(String id, int rating, int secondsAgo) {
        index.add(GAME, REGION, new WaitingPlayer(
                id, "Player " + id, REGION, rating, NOW.minusSeconds(secondsAgo)));
    }

    @Test
    void emptyQueueFormsNothing() {
        assertThat(matchmaker.formParty(GAME, REGION, 2, NOW)).isEmpty();
    }

    @Test
    void aSinglePlayerCannotFillAPairAndStaysQueued() {
        queue("alone", 1200, 0);

        assertThat(matchmaker.formParty(GAME, REGION, 2, NOW)).isEmpty();
        assertThat(index.size(GAME, REGION))
                .as("a failed attempt must not consume the queue")
                .isEqualTo(1);
    }

    @Test
    void matchesTwoPlayersOfSimilarSkill() {
        queue("a", 1200, 0);
        queue("b", 1250, 0);

        Optional<Party> party = matchmaker.formParty(GAME, REGION, 2, NOW);

        assertThat(party).isPresent();
        assertThat(party.get().members()).extracting(WaitingPlayer::playerId)
                .containsExactlyInAnyOrder("a", "b");
        assertThat(party.get().ratingSpread()).isEqualTo(50);
        assertThat(index.size(GAME, REGION))
                .as("matched players leave the queue")
                .isZero();
    }

    @Test
    void refusesAMatchOutsideTheInitialWindow() {
        // 400 points apart, both just arrived, so the window is still 100.
        queue("bronze", 1000, 0);
        queue("diamond", 1400, 0);

        assertThat(matchmaker.formParty(GAME, REGION, 2, NOW)).isEmpty();
        assertThat(index.size(GAME, REGION)).isEqualTo(2);
    }

    @Test
    void acceptsTheSameMatchOnceTheWindowHasWidened() {
        // The identical queue, but the anchor has now been waiting 12 seconds,
        // which widens its window to 100 + 25*12 = 400.
        queue("bronze", 1000, 12);
        queue("diamond", 1400, 0);

        Optional<Party> party = matchmaker.formParty(GAME, REGION, 2, NOW);

        assertThat(party).isPresent();
        assertThat(party.get().ratingSpread()).isEqualTo(400);
        assertThat(party.get().anchorWait()).isEqualTo(Duration.ofSeconds(12));
    }

    @Test
    void anchorsOnTheLongestWaitingPlayer() {
        queue("waited-longest", 1000, 120);
        queue("just-arrived", 1980, 0);
        queue("near-the-anchor", 1050, 1);

        Party party = matchmaker.formParty(GAME, REGION, 2, NOW).orElseThrow();

        assertThat(party.anchor().playerId()).isEqualTo("waited-longest");
        assertThat(party.members()).extracting(WaitingPlayer::playerId)
                .containsExactly("waited-longest", "near-the-anchor");
    }

    @Test
    void picksTheClosestCandidateNotTheFirstOneFound() {
        queue("anchor", 1500, 30);
        queue("far", 1200, 20);
        queue("closest", 1490, 10);
        queue("mid", 1350, 15);

        Party party = matchmaker.formParty(GAME, REGION, 2, NOW).orElseThrow();

        assertThat(party.members()).extracting(WaitingPlayer::playerId)
                .containsExactly("anchor", "closest");
        assertThat(party.ratingSpread()).isEqualTo(10);
    }

    @Test
    void fillsAFourPlayerParty() {
        queue("a", 1500, 40);
        queue("b", 1520, 30);
        queue("c", 1480, 20);
        queue("d", 1510, 10);
        queue("outlier", 900, 5);

        Party party = matchmaker.formParty(GAME, REGION, 4, NOW).orElseThrow();

        assertThat(party.size()).isEqualTo(4);
        assertThat(party.members()).extracting(WaitingPlayer::playerId)
                .doesNotContain("outlier");
        assertThat(party.ratingSpread()).isEqualTo(40);
        assertThat(index.size(GAME, REGION))
                .as("only the outlier is left waiting")
                .isEqualTo(1);
    }

    @Test
    void aPartyOfOneIsJustTheAnchor() {
        queue("solo", 1200, 5);

        Party party = matchmaker.formParty(GAME, REGION, 1, NOW).orElseThrow();

        assertThat(party.size()).isEqualTo(1);
        assertThat(party.ratingSpread()).isZero();
        assertThat(index.size(GAME, REGION)).isZero();
    }

    @Test
    void repeatedMatchingDrainsTheQueueOldestFirst() {
        queue("first", 1200, 50);
        queue("second", 1210, 40);
        queue("third", 1220, 30);
        queue("fourth", 1230, 20);

        Party one = matchmaker.formParty(GAME, REGION, 2, NOW).orElseThrow();
        Party two = matchmaker.formParty(GAME, REGION, 2, NOW).orElseThrow();

        assertThat(one.anchor().playerId()).isEqualTo("first");
        assertThat(two.anchor().playerId()).isEqualTo("third");
        assertThat(matchmaker.formParty(GAME, REGION, 2, NOW)).isEmpty();
        assertThat(index.size(GAME, REGION)).isZero();
    }

    @Test
    void rejectsANonsensePartySize() {
        assertThatThrownBy(() -> matchmaker.formParty(GAME, REGION, 0, NOW))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
