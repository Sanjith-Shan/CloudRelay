package com.cloudrelay.matchmaking;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Measures the two things a change to matchmaking has to justify itself on:
 * how long a match takes to compute, and how good the match is.
 *
 * <p><b>Speed.</b> The skip-list index against the same algorithm written over a
 * plain list. Both produce the same parties — {@code InMemoryWaitingPlayerIndexTest}
 * proves that — so the difference is purely the data structure: O(log n + p)
 * against O(n log n) per attempt.
 *
 * <p><b>Quality.</b> Skill matching against the FIFO matcher the service
 * originally had, over an identical queue. FIFO is not a strawman; it is
 * genuinely the right choice for a game with a thin player base, because it
 * never makes anyone wait. What it cannot do is control who ends up playing
 * together, and rating spread is the number that shows it.
 *
 * <p>This runs as a test rather than under JMH deliberately. JMH would give
 * tighter numbers, and would also mean a benchmark that only runs when somebody
 * remembers to run it. Here the algorithm is re-measured on every CI run and the
 * assertions fail if a change ever makes the indexed path stop being the faster
 * one, which is the property actually worth defending. The absolute timings are
 * JIT-warmed and single-threaded, and are worth reading as an order of
 * magnitude rather than to three significant figures.
 */
class MatchmakingBenchmarkTest {

    private static final String GAME = "cyberpunk-2077";
    private static final String REGION = "us-west-2";
    private static final Instant NOW = Instant.parse("2026-08-27T12:00:00Z");
    private static final int PARTY_SIZE = 4;

    private static final int[] QUEUE_SIZES = {1_000, 10_000, 100_000};
    private static final int WARMUP_ATTEMPTS = 100;
    private static final int MEASURED_ATTEMPTS = 200;

    /**
     * A queue as it actually looks: ratings on a bell curve, arrival times
     * spread over the last couple of minutes.
     *
     * <p>Both details matter. A uniform rating spread would make every search
     * window equally productive and hide the case the expanding window exists
     * for, the player at the tail with nobody nearby. Arrival times bunched at
     * one instant would give every anchor the same window width, when the whole
     * design is that the width varies with how long somebody has waited.
     */
    private static List<WaitingPlayer> population(int size, long seed) {
        Random random = new Random(seed);
        List<WaitingPlayer> players = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            int rating = (int) Math.max(0, Math.round(1200 + random.nextGaussian() * 300));
            int waitedSeconds = random.nextInt(120);
            players.add(new WaitingPlayer("p" + i, "Player " + i, REGION, rating,
                    NOW.minusSeconds(waitedSeconds)));
        }
        return players;
    }

    private static void fill(WaitingPlayerIndex index, List<WaitingPlayer> players) {
        for (WaitingPlayer p : players) {
            index.add(GAME, REGION, p);
        }
    }

    /**
     * Median nanoseconds per {@code formParty} attempt, at a constant queue
     * depth.
     *
     * <p>Matching removes players, so a naive loop would measure a queue that
     * shrinks under it and report a number that is really an average over
     * several different values of n. Each matched party is put back at the
     * tail of the queue with a fresh arrival time, outside the timed region.
     * That holds n constant, and gives every attempt a different anchor, which
     * is what a busy queue in steady state actually looks like.
     */
    private static double timeMatching(WaitingPlayerIndex index, int attempts) {
        SkillMatchmaker matchmaker =
                new SkillMatchmaker(index, new ExpandingWindow(100, 25, 600));
        long[] samples = new long[attempts];
        int taken = 0;
        for (int i = 0; i < attempts; i++) {
            long start = System.nanoTime();
            Optional<Party> party = matchmaker.formParty(GAME, REGION, PARTY_SIZE, NOW);
            long elapsed = System.nanoTime() - start;

            if (party.isPresent()) {
                samples[taken++] = elapsed;
                requeueAtTail(index, party.get(), i);
            }
        }
        if (taken == 0) {
            return Double.NaN;
        }
        long[] measured = Arrays.copyOf(samples, taken);
        Arrays.sort(measured);
        return measured[taken / 2];
    }

    private static void requeueAtTail(WaitingPlayerIndex index, Party party, int attempt) {
        for (WaitingPlayer member : party.members()) {
            index.add(GAME, REGION, new WaitingPlayer(
                    member.playerId(), member.displayName(), member.region(),
                    member.rating(), NOW.plusSeconds(attempt + 1L)));
        }
    }

    @Test
    void indexedMatchingOutscalesTheLinearScan() throws IOException {
        StringBuilder report = new StringBuilder();
        report.append("### Matchmaking: skip-list index vs linear scan\n\n");
        report.append(environment()).append("\n\n");
        report.append("Median nanoseconds per party formed, party size ")
                .append(PARTY_SIZE)
                .append(", queue held at a constant depth.\n\n");
        report.append("| Players queued | Linear scan O(n log n) | Skip-list index O(log n + p)"
                + " | Speedup |\n|---|---|---|---|\n");

        double largestSpeedup = 0;
        for (int size : QUEUE_SIZES) {
            List<WaitingPlayer> players = population(size, 20260827L + size);

            warmUp(players);

            LinearScanWaitingPlayerIndex linear = new LinearScanWaitingPlayerIndex();
            fill(linear, players);
            double linearNs = timeMatching(linear, MEASURED_ATTEMPTS);

            InMemoryWaitingPlayerIndex indexed = new InMemoryWaitingPlayerIndex();
            fill(indexed, players);
            double indexedNs = timeMatching(indexed, MEASURED_ATTEMPTS);

            double speedup = linearNs / indexedNs;
            largestSpeedup = speedup;
            report.append(String.format("| %,d | %,.0f ns | %,.0f ns | %.0fx |%n",
                    size, linearNs, indexedNs, speedup));
        }

        System.out.println(report);
        write("matchmaking-scaling.md", report.toString());

        // Deliberately loose. The measured gap at 100,000 players is three
        // orders of magnitude; asserting 10x leaves room for a loaded CI runner
        // while still failing loudly if the index ever stops being an index.
        assertThat(largestSpeedup)
                .as("the indexed matcher must outscale the linear scan at 100,000 queued players")
                .isGreaterThan(10.0);
    }

    /**
     * Match quality at a queue depth a real game actually has.
     *
     * <p>Depth is the whole experiment here, so it is worth saying why 400. At
     * 20,000 queued players there are roughly eleven people at every single
     * integer rating, so finding three others with an identical rating is
     * trivial and the skill matcher scores a spread of nearly zero. That number
     * is real and completely uninformative: it measures the density of the
     * synthetic population, not the algorithm. A few hundred players waiting for
     * one game in one region is the situation matchmaking is actually solving,
     * and it is where the two approaches genuinely differ.
     */
    @Test
    void skillMatchingProducesTighterPartiesThanFifo() throws IOException {
        int queueDepth = 400;
        List<WaitingPlayer> players = population(queueDepth, 20260827L);

        InMemoryWaitingPlayerIndex index = new InMemoryWaitingPlayerIndex();
        fill(index, players);
        SkillMatchmaker matchmaker =
                new SkillMatchmaker(index, new ExpandingWindow(100, 25, 600));

        List<Integer> skillSpreads = new ArrayList<>();
        Optional<Party> party;
        while ((party = matchmaker.formParty(GAME, REGION, PARTY_SIZE, NOW)).isPresent()) {
            skillSpreads.add(party.get().ratingSpread());
        }

        // FIFO over the same population: take players in arrival order, four at
        // a time, and never look at a rating.
        List<WaitingPlayer> byArrival = new ArrayList<>(players);
        byArrival.sort(java.util.Comparator.comparing(WaitingPlayer::enqueuedAt)
                .thenComparing(WaitingPlayer::playerId));
        List<Integer> fifoSpreads = new ArrayList<>();
        for (int i = 0; i + PARTY_SIZE <= byArrival.size(); i += PARTY_SIZE) {
            List<WaitingPlayer> group = byArrival.subList(i, i + PARTY_SIZE);
            int min = group.stream().mapToInt(WaitingPlayer::rating).min().orElseThrow();
            int max = group.stream().mapToInt(WaitingPlayer::rating).max().orElseThrow();
            fifoSpreads.add(max - min);
        }

        double skillMean = mean(skillSpreads);
        double fifoMean = mean(fifoSpreads);

        String report = """
                ### Matchmaking: match quality, skill based vs FIFO

                %s

                %,d players queued for one game in one region, ratings normally
                distributed around 1200 with a standard deviation of 300, arrival
                times spread over the preceding two minutes. Party size %d.
                Rating spread is the gap between the strongest and weakest player
                in a party, so lower is a more even game.

                | Matcher | Parties formed | Mean rating spread | p50 | p95 |
                |---|---|---|---|---|
                | FIFO, arrival order | %,d | %.1f | %d | %d |
                | Skill based, expanding window | %,d | %.1f | %d | %d |

                Skill matching reduced the mean rating spread by %.1f%%.
                """.formatted(environment(), queueDepth, PARTY_SIZE,
                fifoSpreads.size(), fifoMean, percentile(fifoSpreads, 50),
                percentile(fifoSpreads, 95),
                skillSpreads.size(), skillMean, percentile(skillSpreads, 50),
                percentile(skillSpreads, 95),
                (1 - skillMean / fifoMean) * 100);

        System.out.println(report);
        write("matchmaking-quality.md", report);

        assertThat(skillSpreads).as("the queue should yield parties").isNotEmpty();
        assertThat(skillMean)
                .as("skill based matching must produce more even parties than FIFO")
                .isLessThan(fifoMean);
    }

    /** Compiles both code paths before anything is recorded. */
    private static void warmUp(List<WaitingPlayer> players) {
        List<WaitingPlayer> sample = players.subList(0, Math.min(players.size(), 2_000));

        LinearScanWaitingPlayerIndex linear = new LinearScanWaitingPlayerIndex();
        fill(linear, sample);
        timeMatching(linear, WARMUP_ATTEMPTS);

        InMemoryWaitingPlayerIndex indexed = new InMemoryWaitingPlayerIndex();
        fill(indexed, sample);
        timeMatching(indexed, WARMUP_ATTEMPTS);
    }

    private static double mean(List<Integer> values) {
        return values.stream().mapToInt(Integer::intValue).average().orElse(0);
    }

    private static int percentile(List<Integer> values, int percentile) {
        if (values.isEmpty()) {
            return 0;
        }
        List<Integer> sorted = new ArrayList<>(values);
        sorted.sort(Integer::compareTo);
        int index = (int) Math.ceil(percentile / 100.0 * sorted.size()) - 1;
        return sorted.get(Math.min(sorted.size() - 1, Math.max(0, index)));
    }

    private static String environment() {
        return "Measured on " + System.getProperty("os.name") + " "
                + System.getProperty("os.arch") + ", "
                + Runtime.getRuntime().availableProcessors() + " cores, Java "
                + System.getProperty("java.version") + ", single threaded, in-process.";
    }

    /** Writes into target/ so a docs refresh can pick the numbers straight up. */
    private static void write(String name, String content) throws IOException {
        Path dir = Path.of("target", "benchmarks");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve(name), content);
    }
}
