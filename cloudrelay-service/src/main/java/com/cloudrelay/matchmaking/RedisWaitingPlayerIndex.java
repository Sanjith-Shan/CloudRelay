package com.cloudrelay.matchmaking;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * The queue in Redis, so every replica of the service matches against the same
 * players.
 *
 * <p>Three keys per game and region:
 *
 * <pre>
 *   skillq:{game}:{region}:rating   ZSET  member = playerId, score = rating
 *   skillq:{game}:{region}:time     ZSET  member = playerId, score = enqueue millis
 *   skillq:{game}:{region}:players  HASH  playerId -&gt; WaitingPlayer as JSON
 * </pre>
 *
 * <p>The player id is the member of both sorted sets and the record lives in a
 * hash beside them. Putting the serialised record in the sorted set instead
 * would make removal depend on Jackson producing byte-identical output for a
 * value that has already been through a round trip, which is a fragile thing to
 * build a queue on. Keying by id makes removal exact.
 *
 * <p>A Redis sorted set is a skip list with a hash index alongside it, which is
 * why the costs here match {@link InMemoryWaitingPlayerIndex} operation for
 * operation: {@code ZADD} and {@code ZREM} are O(log n), and
 * {@code ZRANGEBYSCORE} with a {@code LIMIT} is O(log n + k).
 */
@Slf4j
public final class RedisWaitingPlayerIndex implements WaitingPlayerIndex {

    private static final String PREFIX = "skillq:";

    /**
     * Queues are dropped after an hour of no activity. A player who abandoned
     * the client mid-queue is not coming back, and without this their entry
     * would sit at the head of the arrival ordering forever, anchoring every
     * future attempt to somebody who can never be matched.
     */
    private static final Duration QUEUE_TTL = Duration.ofHours(1);

    private final RedisTemplate<String, Object> redisTemplate;

    /**
     * A private mapper, and the record is stored as a JSON string rather than
     * as an object.
     *
     * <p>The shared Redis serializer adds type information only for non-final
     * classes, and {@link WaitingPlayer} is a record, which is final. Handed to
     * it directly the record would serialise cleanly and come back as a
     * {@code LinkedHashMap}, at runtime, in production. Encoding to a string
     * here keeps this class's wire format its own business and independent of
     * how the cache happens to be configured.
     */
    private final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    public RedisWaitingPlayerIndex(RedisTemplate<String, Object> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    private String ratingKey(String gameId, String region) {
        return PREFIX + gameId + ":" + region + ":rating";
    }

    private String timeKey(String gameId, String region) {
        return PREFIX + gameId + ":" + region + ":time";
    }

    private String playersKey(String gameId, String region) {
        return PREFIX + gameId + ":" + region + ":players";
    }

    @Override
    public void add(String gameId, String region, WaitingPlayer player) {
        String rating = ratingKey(gameId, region);
        String time = timeKey(gameId, region);
        String players = playersKey(gameId, region);

        redisTemplate.opsForZSet().add(rating, player.playerId(), player.rating());
        redisTemplate.opsForZSet().add(time, player.playerId(),
                player.enqueuedAt().toEpochMilli());
        redisTemplate.opsForHash().put(players, player.playerId(), encode(player));

        redisTemplate.expire(rating, QUEUE_TTL);
        redisTemplate.expire(time, QUEUE_TTL);
        redisTemplate.expire(players, QUEUE_TTL);
    }

    @Override
    public Optional<WaitingPlayer> longestWaiting(String gameId, String region) {
        Set<Object> head = redisTemplate.opsForZSet().range(timeKey(gameId, region), 0, 0);
        if (head == null || head.isEmpty()) {
            return Optional.empty();
        }
        Object playerId = head.iterator().next();
        return Optional.ofNullable(lookup(gameId, region, String.valueOf(playerId)));
    }

    /**
     * Two bounded range scans outward from the middle of the band, merged by
     * distance.
     *
     * <p>Redis has no "nearest to a score" query, but it has a descending range
     * and an ascending range, both with a LIMIT. Running one of each from the
     * midpoint and merging them reproduces the outward walk the in-memory index
     * does, and keeps the work proportional to the number of candidates wanted
     * rather than to the width of the window. Asking for the whole band and
     * sorting in the service would move an unbounded amount of data across the
     * network on every attempt.
     *
     * <p>The scores come back with the members, so the merge compares numbers
     * already in hand. Looking each score up separately would turn a two round
     * trip operation into one round trip per comparison, which would cost more
     * than the sort it was avoiding.
     */
    @Override
    public List<WaitingPlayer> withinRating(String gameId, String region,
                                            int minRating, int maxRating, int limit) {
        String key = ratingKey(gameId, region);
        double midpoint = minRating + (maxRating - minRating) / 2.0;
        ZSetOperations<String, Object> zset = redisTemplate.opsForZSet();

        List<TypedTuple<Object>> below = asList(
                zset.reverseRangeByScoreWithScores(key, minRating, midpoint, 0, limit));
        List<TypedTuple<Object>> above = asList(
                zset.rangeByScoreWithScores(key, midpoint, maxRating, 0, limit));

        // A player whose rating is exactly the midpoint is returned by both
        // scans, so the merge deduplicates by id rather than trusting the
        // ranges to be disjoint.
        Map<String, Object> ordered = new LinkedHashMap<>();
        int i = 0;
        int j = 0;
        while (ordered.size() < limit && (i < below.size() || j < above.size())) {
            boolean takeBelow;
            if (j >= above.size()) {
                takeBelow = true;
            } else if (i >= below.size()) {
                takeBelow = false;
            } else {
                double loDistance = midpoint - score(below.get(i));
                double hiDistance = score(above.get(j)) - midpoint;
                takeBelow = loDistance <= hiDistance;
            }
            TypedTuple<Object> chosen = takeBelow ? below.get(i++) : above.get(j++);
            ordered.putIfAbsent(String.valueOf(chosen.getValue()), chosen.getValue());
        }

        List<WaitingPlayer> out = new ArrayList<>(ordered.size());
        for (String playerId : ordered.keySet()) {
            WaitingPlayer player = lookup(gameId, region, playerId);
            if (player != null) {
                out.add(player);
            }
        }
        return out;
    }

    @Override
    public void removeAll(String gameId, String region, Collection<WaitingPlayer> players) {
        if (players.isEmpty()) {
            return;
        }
        Object[] ids = players.stream().map(WaitingPlayer::playerId).toArray();
        redisTemplate.opsForZSet().remove(ratingKey(gameId, region), ids);
        redisTemplate.opsForZSet().remove(timeKey(gameId, region), ids);
        redisTemplate.opsForHash().delete(playersKey(gameId, region), ids);
    }

    @Override
    public long size(String gameId, String region) {
        Long size = redisTemplate.opsForZSet().zCard(timeKey(gameId, region));
        return size == null ? 0 : size;
    }

    /** Empties one game and region queue. Used by the demo script and tests. */
    public void clear(String gameId, String region) {
        redisTemplate.delete(List.of(
                ratingKey(gameId, region),
                timeKey(gameId, region),
                playersKey(gameId, region)));
    }

    private static List<TypedTuple<Object>> asList(Set<TypedTuple<Object>> tuples) {
        return tuples == null ? List.of() : new ArrayList<>(tuples);
    }

    private static double score(TypedTuple<Object> tuple) {
        Double score = tuple.getScore();
        return score == null ? 0 : score;
    }

    private String encode(WaitingPlayer player) {
        try {
            return mapper.writeValueAsString(player);
        } catch (Exception ex) {
            throw new IllegalStateException("Could not encode waiting player", ex);
        }
    }

    private WaitingPlayer lookup(String gameId, String region, String playerId) {
        Object value = redisTemplate.opsForHash().get(playersKey(gameId, region), playerId);
        if (value == null) {
            return null;
        }
        try {
            return mapper.readValue(String.valueOf(value), WaitingPlayer.class);
        } catch (Exception ex) {
            // A record that cannot be decoded is one player who will not be
            // matched, not a reason to fail the whole attempt for everyone else
            // in the queue.
            log.warn("Could not decode queued player {}", playerId, ex);
            return null;
        }
    }
}
