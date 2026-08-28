package com.cloudrelay.lakehouse;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * Generates session event JSON in the exact shape
 * {@code SessionEventPublisher} writes to Kafka.
 *
 * <p>The envelope is built by hand rather than by importing the service module.
 * Putting Spring Boot on the same test classpath as Spark is a dependency fight
 * nobody needs, and the lakehouse should be able to build and be tested without
 * the producer. The contract between the two is held by a pair of tests instead:
 * {@code SessionEventSchemaTest} here and {@code AnalyticsWireFormatTest} in the
 * service both assert the same field list, so a change on either side fails a
 * build.
 *
 * <p>What it generates is a lifecycle, not a pile of unrelated rows: a session
 * is created, players join it, most sessions start, some are abandoned, and
 * they terminate. The gold aggregates measure funnels and durations, and those
 * are meaningless over events that do not form sessions.
 */
public final class TestEvents {

    public static final List<String> GAMES =
            List.of("cyberpunk-2077", "fortnite", "rocket-league", "apex-legends");
    public static final List<String> REGIONS =
            List.of("us-west-2", "us-east-1", "eu-central-1", "ap-northeast-1");

    private TestEvents() {
    }

    /** One generated session and the events it produced. */
    public record GeneratedSession(String sessionCode, List<String> events, boolean started) {
    }

    /**
     * @param sessions how many session lifecycles to generate
     * @param startAt  the first event's timestamp; later events are spread
     *                 forward from it so that per-minute windows have content
     */
    public static List<GeneratedSession> sessions(int sessions, Instant startAt, long seed) {
        Random random = new Random(seed);
        List<GeneratedSession> out = new ArrayList<>(sessions);

        for (int i = 0; i < sessions; i++) {
            String code = String.format("S%07d", i);
            String game = GAMES.get(random.nextInt(GAMES.size()));
            String region = REGIONS.get(random.nextInt(REGIONS.size()));
            int maxPlayers = 4;
            int joins = 1 + random.nextInt(maxPlayers);
            // Spread creations over 10 minutes so the one minute rollups have
            // more than a single bucket to aggregate into.
            Instant createdAt = startAt.plusSeconds(random.nextInt(600))
                    .truncatedTo(ChronoUnit.MILLIS);

            List<String> events = new ArrayList<>();
            int playerCount = 1;
            events.add(event("SESSION_CREATED", code, game, region, "WAITING",
                    playerCount, maxPlayers, createdAt, createdAt, 0));

            Instant at = createdAt;
            for (int j = 1; j < joins; j++) {
                at = at.plusSeconds(1 + random.nextInt(20));
                playerCount++;
                String state = playerCount >= 2 ? "STARTING" : "WAITING";
                events.add(event("PLAYER_JOINED", code, game, region, state,
                        playerCount, maxPlayers, createdAt, at,
                        at.getEpochSecond() - createdAt.getEpochSecond()));
            }

            // Four in five sessions get going. The rest are the abandoned
            // lobbies the funnel table exists to count.
            boolean started = playerCount >= 2 && random.nextInt(5) > 0;
            if (started) {
                at = at.plusSeconds(1 + random.nextInt(30));
                events.add(event("SESSION_STARTED", code, game, region, "ACTIVE",
                        playerCount, maxPlayers, createdAt, at,
                        at.getEpochSecond() - createdAt.getEpochSecond()));

                at = at.plusSeconds(60 + random.nextInt(1800));
            } else {
                at = at.plusSeconds(30 + random.nextInt(120));
            }

            events.add(event("SESSION_TERMINATED", code, game, region, "TERMINATED",
                    playerCount, maxPlayers, createdAt, at,
                    at.getEpochSecond() - createdAt.getEpochSecond()));

            out.add(new GeneratedSession(code, events, started));
        }
        return out;
    }

    /** Flattens generated sessions to the event lines a producer would emit. */
    public static List<String> flatten(List<GeneratedSession> sessions) {
        List<String> lines = new ArrayList<>();
        for (GeneratedSession s : sessions) {
            lines.addAll(s.events());
        }
        return lines;
    }

    public static List<String> lines(int sessions, Instant startAt, long seed) {
        return flatten(sessions(sessions, startAt, seed));
    }

    /**
     * One event, serialised exactly as the service does: ISO-8601 timestamps at
     * millisecond precision, a UUID event id, and the full session payload.
     */
    public static String event(String type, String sessionCode, String gameId, String region,
                               String state, int playerCount, int maxPlayers,
                               Instant createdAt, Instant eventTime, long uptimeSeconds) {
        String host = "\"host\":{\"playerId\":\"" + sessionCode + "-p0\",\"displayName\":\"Host\","
                + "\"region\":\"" + region + "\",\"joinedAt\":\"" + iso(createdAt)
                + "\",\"connected\":true}";

        StringBuilder players = new StringBuilder("\"players\":[");
        for (int i = 0; i < playerCount; i++) {
            if (i > 0) {
                players.append(',');
            }
            players.append("{\"playerId\":\"").append(sessionCode).append("-p").append(i)
                    .append("\",\"displayName\":\"Player ").append(i)
                    .append("\",\"region\":\"").append(region)
                    .append("\",\"joinedAt\":\"").append(iso(createdAt))
                    .append("\",\"connected\":true}");
        }
        players.append(']');

        return "{"
                + "\"eventId\":\"" + UUID.randomUUID() + "\","
                + "\"eventTime\":\"" + iso(eventTime) + "\","
                + "\"type\":\"" + type + "\","
                + "\"sessionCode\":\"" + sessionCode + "\","
                + "\"schemaVersion\":" + SessionEventSchema.CURRENT_SCHEMA_VERSION + ","
                + "\"session\":{"
                + "\"id\":\"" + sessionCode + "-id\","
                + "\"sessionCode\":\"" + sessionCode + "\","
                + "\"gameId\":\"" + gameId + "\","
                + "\"region\":\"" + region + "\","
                + "\"state\":\"" + state + "\","
                + host + ","
                + players + ","
                + "\"maxPlayers\":" + maxPlayers + ","
                + "\"minPlayersToStart\":2,"
                + "\"createdAt\":\"" + iso(createdAt) + "\","
                + "\"updatedAt\":\"" + iso(eventTime) + "\","
                + "\"expiresAt\":\"" + iso(createdAt.plusSeconds(7200)) + "\","
                + "\"currentPlayerCount\":" + playerCount + ","
                + "\"isFull\":" + (playerCount >= maxPlayers) + ","
                + "\"uptimeSeconds\":" + uptimeSeconds
                + "}}";
    }

    private static String iso(Instant instant) {
        return instant.truncatedTo(ChronoUnit.MILLIS).toString();
    }

    /**
     * Writes event lines into the landing directory, one file per chunk.
     *
     * <p>File count is the control the restart test needs. Spark's file source
     * reads whole files, so {@code maxFilesPerTrigger} plus a known number of
     * files makes micro-batch boundaries deterministic and lets a kill be aimed
     * at a specific point in the stream instead of at whenever a timer happened
     * to fire.
     */
    public static void writeLanding(Path landingDir, List<String> lines, int filesToWrite)
            throws IOException {
        Files.createDirectories(landingDir);
        int perFile = Math.max(1, (int) Math.ceil(lines.size() / (double) filesToWrite));
        int fileIndex = 0;
        for (int i = 0; i < lines.size(); i += perFile) {
            List<String> chunk = lines.subList(i, Math.min(lines.size(), i + perFile));
            Files.write(landingDir.resolve(String.format("events-%04d.json", fileIndex++)),
                    chunk, StandardCharsets.UTF_8);
        }
    }
}
