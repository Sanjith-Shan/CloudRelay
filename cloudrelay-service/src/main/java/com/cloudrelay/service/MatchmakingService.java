package com.cloudrelay.service;

import com.cloudrelay.dto.CreateSessionRequest;
import com.cloudrelay.dto.JoinSessionRequest;
import com.cloudrelay.dto.MatchRequest;
import com.cloudrelay.dto.SessionResponse;
import com.cloudrelay.model.GameSession;
import com.cloudrelay.model.SessionState;
import com.cloudrelay.repository.SessionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Optional;

/**
 * Region aware matchmaking backed by a Redis list per game and region.
 * Players enqueue with LPUSH and the matcher dequeues the oldest waiting
 * player with RPOP, then places them into an open session or creates one.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class MatchmakingService {

    private static final String QUEUE_KEY_PREFIX = "matchqueue:";

    private final SessionRepository sessionRepository;
    private final RedisTemplate<String, Object> redisTemplate;
    private final SessionService sessionService;

    public String enqueue(MatchRequest request) {
        String key = queueKey(request.getGameId(), request.getRegion());
        redisTemplate.opsForList().leftPush(key, request);
        Long depth = redisTemplate.opsForList().size(key);
        long position = depth == null ? 1 : depth;
        log.info("Player {} enqueued for game {} in region {} at position {}",
                request.getPlayerId(), request.getGameId(), request.getRegion(), position);
        return "Player " + request.getPlayerId() + " queued at position " + position;
    }

    public Optional<SessionResponse> tryMatch(String gameId, String region) {
        String key = queueKey(gameId, region);
        Object popped = redisTemplate.opsForList().rightPop(key);
        if (!(popped instanceof MatchRequest request)) {
            return Optional.empty();
        }

        Optional<GameSession> openSession = sessionRepository
                .findByGameIdAndRegionAndState(gameId, region, SessionState.WAITING)
                .stream()
                .filter(s -> s.getPlayers().size() < s.getMaxPlayers())
                .findFirst();

        if (openSession.isPresent()) {
            String code = openSession.get().getSessionCode();
            log.info("Matched player {} into existing session {}", request.getPlayerId(), code);
            return Optional.of(sessionService.joinSession(code, JoinSessionRequest.builder()
                    .playerId(request.getPlayerId())
                    .displayName(request.getDisplayName())
                    .region(request.getRegion())
                    .build()));
        }

        log.info("No open session for game {} in region {}, creating one for player {}",
                gameId, region, request.getPlayerId());
        return Optional.of(sessionService.createSession(CreateSessionRequest.builder()
                .gameId(request.getGameId())
                .playerId(request.getPlayerId())
                .displayName(request.getDisplayName())
                .region(request.getRegion())
                .build()));
    }

    public long getQueueDepth(String gameId, String region) {
        Long depth = redisTemplate.opsForList().size(queueKey(gameId, region));
        return depth == null ? 0 : depth;
    }

    private String queueKey(String gameId, String region) {
        return QUEUE_KEY_PREFIX + gameId + ":" + region;
    }
}
