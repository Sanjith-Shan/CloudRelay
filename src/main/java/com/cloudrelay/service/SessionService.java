package com.cloudrelay.service;

import com.cloudrelay.dto.CreateSessionRequest;
import com.cloudrelay.dto.JoinSessionRequest;
import com.cloudrelay.dto.SessionResponse;
import com.cloudrelay.event.SessionEventPublisher;
import com.cloudrelay.exception.SessionFullException;
import com.cloudrelay.exception.SessionNotFoundException;
import com.cloudrelay.model.GameSession;
import com.cloudrelay.model.Player;
import com.cloudrelay.model.SessionState;
import com.cloudrelay.repository.SessionRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

@Slf4j
@Service
public class SessionService {

    private static final String CACHE_KEY_PREFIX = "session:";
    private static final Duration CACHE_TTL = Duration.ofMinutes(30);
    private static final String CODE_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    private static final int MAX_CODE_ATTEMPTS = 10;
    private static final int MAX_JOIN_RETRIES = 3;
    private static final EnumSet<SessionState> JOINABLE_STATES =
            EnumSet.of(SessionState.WAITING, SessionState.STARTING, SessionState.ACTIVE);
    private static final List<SessionState> LIVE_STATES = List.of(
            SessionState.WAITING, SessionState.STARTING, SessionState.ACTIVE, SessionState.PAUSED);

    private final SessionRepository sessionRepository;
    private final RedisTemplate<String, Object> redisTemplate;
    private final SessionEventPublisher eventPublisher;
    private final SecureRandom random = new SecureRandom();

    private final int sessionCodeLength;
    private final int waitingTtlHours;
    private final int activeTtlHours;

    private final Counter sessionsCreatedCounter;
    private final Timer joinLatencyTimer;
    private final MeterRegistry meterRegistry;

    public SessionService(SessionRepository sessionRepository,
                          RedisTemplate<String, Object> redisTemplate,
                          MeterRegistry meterRegistry,
                          SessionEventPublisher eventPublisher,
                          @Value("${cloudrelay.session.session-code-length:8}") int sessionCodeLength,
                          @Value("${cloudrelay.session.waiting-ttl-hours:2}") int waitingTtlHours,
                          @Value("${cloudrelay.session.active-ttl-hours:6}") int activeTtlHours) {
        this.sessionRepository = sessionRepository;
        this.redisTemplate = redisTemplate;
        this.meterRegistry = meterRegistry;
        this.eventPublisher = eventPublisher;
        this.sessionCodeLength = sessionCodeLength;
        this.waitingTtlHours = waitingTtlHours;
        this.activeTtlHours = activeTtlHours;

        this.sessionsCreatedCounter = Counter.builder("cloudrelay.sessions.created")
                .description("Total number of game sessions created")
                .register(meterRegistry);
        this.joinLatencyTimer = Timer.builder("cloudrelay.session.join.latency")
                .description("Latency of session join operations")
                .register(meterRegistry);
        Gauge.builder("cloudrelay.sessions.active", this, SessionService::countLiveSessions)
                .description("Number of sessions in a live state")
                .register(meterRegistry);
        Gauge.builder("cloudrelay.players.connected", this, SessionService::countConnectedPlayers)
                .description("Number of players in live sessions")
                .register(meterRegistry);
    }

    public SessionResponse createSession(CreateSessionRequest request) {
        String sessionCode = generateUniqueSessionCode();
        Instant now = Instant.now();

        Player host = Player.builder()
                .playerId(request.getPlayerId())
                .displayName(request.getDisplayName())
                .region(request.getRegion())
                .joinedAt(now)
                .connected(true)
                .build();

        List<Player> players = new ArrayList<>();
        players.add(host);

        GameSession session = GameSession.builder()
                .sessionCode(sessionCode)
                .gameId(request.getGameId())
                .region(request.getRegion())
                .state(SessionState.WAITING)
                .host(host)
                .players(players)
                .maxPlayers(request.getMaxPlayers() == null ? 4 : request.getMaxPlayers())
                .minPlayersToStart(request.getMinPlayersToStart() == null ? 2 : request.getMinPlayersToStart())
                .createdAt(now)
                .updatedAt(now)
                .expiresAt(now.plus(waitingTtlHours, ChronoUnit.HOURS))
                .metadata(request.getMetadata())
                .build();

        GameSession saved = sessionRepository.save(session);
        cacheSession(saved);
        sessionsCreatedCounter.increment();
        log.info("Session {} created for game {} in region {} by player {}",
                sessionCode, request.getGameId(), request.getRegion(), request.getPlayerId());

        SessionResponse response = SessionResponse.from(saved);
        eventPublisher.publish("SESSION_CREATED", response);
        return response;
    }

    public SessionResponse getSession(String sessionCode) {
        return SessionResponse.from(getSessionEntity(sessionCode));
    }

    public SessionResponse joinSession(String sessionCode, JoinSessionRequest request) {
        Timer.Sample sample = Timer.start(meterRegistry);
        try {
            for (int attempt = 1; ; attempt++) {
                try {
                    return doJoin(sessionCode, request);
                } catch (OptimisticLockingFailureException ex) {
                    if (attempt >= MAX_JOIN_RETRIES) {
                        throw new IllegalStateException(
                                "Session is being modified concurrently, please retry");
                    }
                    log.debug("Optimistic lock conflict joining {}, attempt {}", sessionCode, attempt);
                    evictSession(sessionCode);
                }
            }
        } finally {
            sample.stop(joinLatencyTimer);
        }
    }

    private SessionResponse doJoin(String sessionCode, JoinSessionRequest request) {
        GameSession session = getSessionEntity(sessionCode);

        if (!JOINABLE_STATES.contains(session.getState())) {
            throw new IllegalStateException(
                    "Session " + sessionCode + " is not joinable in state " + session.getState());
        }
        if (session.getPlayers().size() >= session.getMaxPlayers()) {
            throw new SessionFullException(sessionCode, session.getMaxPlayers());
        }
        boolean alreadyJoined = session.getPlayers().stream()
                .anyMatch(p -> Objects.equals(p.getPlayerId(), request.getPlayerId()));
        if (alreadyJoined) {
            throw new IllegalStateException(
                    "Player " + request.getPlayerId() + " already joined session " + sessionCode);
        }

        Player player = Player.builder()
                .playerId(request.getPlayerId())
                .displayName(request.getDisplayName())
                .region(request.getRegion())
                .joinedAt(Instant.now())
                .connected(true)
                .build();
        session.getPlayers().add(player);

        if (session.getState() == SessionState.WAITING
                && session.getPlayers().size() >= session.getMinPlayersToStart()) {
            session.setState(SessionState.STARTING);
        }
        session.setUpdatedAt(Instant.now());

        GameSession saved = sessionRepository.save(session);
        cacheSession(saved);
        log.info("Player {} joined session {}", request.getPlayerId(), sessionCode);

        SessionResponse response = SessionResponse.from(saved);
        eventPublisher.publish("PLAYER_JOINED", response);
        return response;
    }

    public SessionResponse leaveSession(String sessionCode, String playerId) {
        GameSession session = getSessionEntity(sessionCode);

        boolean removed = session.getPlayers()
                .removeIf(p -> Objects.equals(p.getPlayerId(), playerId));
        if (!removed) {
            throw new IllegalStateException(
                    "Player " + playerId + " is not in session " + sessionCode);
        }

        boolean hostLeft = session.getHost() != null
                && Objects.equals(session.getHost().getPlayerId(), playerId);
        if (session.getPlayers().isEmpty()) {
            session.setState(SessionState.TERMINATED);
        } else {
            if (hostLeft) {
                session.setHost(session.getPlayers().get(0));
            }
            if (session.getState() == SessionState.STARTING
                    && session.getPlayers().size() < session.getMinPlayersToStart()) {
                session.setState(SessionState.WAITING);
            }
        }
        session.setUpdatedAt(Instant.now());

        GameSession saved = sessionRepository.save(session);
        if (saved.getState() == SessionState.TERMINATED) {
            evictSession(sessionCode);
        } else {
            cacheSession(saved);
        }
        log.info("Player {} left session {}", playerId, sessionCode);

        SessionResponse response = SessionResponse.from(saved);
        eventPublisher.publish("PLAYER_LEFT", response);
        return response;
    }

    public SessionResponse startSession(String sessionCode, String playerId) {
        GameSession session = getSessionEntity(sessionCode);

        if (session.getHost() == null
                || !Objects.equals(session.getHost().getPlayerId(), playerId)) {
            throw new IllegalStateException("Only the host can start the session");
        }
        if (session.getPlayers().size() < session.getMinPlayersToStart()) {
            throw new IllegalStateException(
                    "Session needs at least " + session.getMinPlayersToStart() + " players to start");
        }

        session.setState(SessionState.ACTIVE);
        session.setExpiresAt(Instant.now().plus(activeTtlHours, ChronoUnit.HOURS));
        session.setUpdatedAt(Instant.now());

        GameSession saved = sessionRepository.save(session);
        cacheSession(saved);
        log.info("Session {} started by host {}", sessionCode, playerId);

        SessionResponse response = SessionResponse.from(saved);
        eventPublisher.publish("SESSION_STARTED", response);
        return response;
    }

    public SessionResponse terminateSession(String sessionCode, String playerId) {
        GameSession session = getSessionEntity(sessionCode);

        if (session.getHost() == null
                || !Objects.equals(session.getHost().getPlayerId(), playerId)) {
            throw new IllegalStateException("Only the host can terminate the session");
        }

        session.setState(SessionState.TERMINATED);
        session.setUpdatedAt(Instant.now());

        GameSession saved = sessionRepository.save(session);
        evictSession(sessionCode);
        log.info("Session {} terminated by host {}", sessionCode, playerId);

        SessionResponse response = SessionResponse.from(saved);
        eventPublisher.publish("SESSION_TERMINATED", response);
        return response;
    }

    public List<SessionResponse> listActiveSessions(String gameId, String region) {
        return Stream.of(SessionState.WAITING, SessionState.ACTIVE)
                .flatMap(state -> sessionRepository
                        .findByGameIdAndRegionAndState(gameId, region, state).stream())
                .map(SessionResponse::from)
                .toList();
    }

    public List<SessionResponse> getPlayerSessions(String playerId) {
        return LIVE_STATES.stream()
                .flatMap(state -> sessionRepository
                        .findByStateAndPlayersPlayerId(state, playerId).stream())
                .map(SessionResponse::from)
                .toList();
    }

    GameSession getSessionEntity(String sessionCode) {
        String key = CACHE_KEY_PREFIX + sessionCode;
        try {
            Object cached = redisTemplate.opsForValue().get(key);
            if (cached instanceof GameSession session) {
                log.debug("Cache hit for session {}", sessionCode);
                return session;
            }
        } catch (Exception ex) {
            log.warn("Redis read failed for {}, falling back to MongoDB", sessionCode, ex);
        }

        GameSession session = sessionRepository.findBySessionCode(sessionCode)
                .orElseThrow(() -> new SessionNotFoundException(sessionCode));
        cacheSession(session);
        return session;
    }

    private void cacheSession(GameSession session) {
        String key = CACHE_KEY_PREFIX + session.getSessionCode();
        try {
            redisTemplate.opsForValue().set(key, session);
            redisTemplate.expire(key, CACHE_TTL);
        } catch (Exception ex) {
            log.warn("Redis write failed for {}", session.getSessionCode(), ex);
        }
    }

    private void evictSession(String sessionCode) {
        try {
            redisTemplate.delete(CACHE_KEY_PREFIX + sessionCode);
        } catch (Exception ex) {
            log.warn("Redis evict failed for {}", sessionCode, ex);
        }
    }

    private String generateUniqueSessionCode() {
        for (int attempt = 0; attempt < MAX_CODE_ATTEMPTS; attempt++) {
            String code = randomCode();
            if (sessionRepository.findBySessionCode(code).isEmpty()) {
                return code;
            }
        }
        throw new IllegalStateException("Unable to generate a unique session code");
    }

    private String randomCode() {
        StringBuilder sb = new StringBuilder(sessionCodeLength);
        for (int i = 0; i < sessionCodeLength; i++) {
            sb.append(CODE_ALPHABET.charAt(random.nextInt(CODE_ALPHABET.length())));
        }
        return sb.toString();
    }

    private double countLiveSessions() {
        try {
            return sessionRepository.findByStateIn(LIVE_STATES).size();
        } catch (Exception ex) {
            return 0;
        }
    }

    private double countConnectedPlayers() {
        try {
            return sessionRepository.findByStateIn(LIVE_STATES).stream()
                    .filter(s -> s.getPlayers() != null)
                    .mapToInt(s -> s.getPlayers().size())
                    .sum();
        } catch (Exception ex) {
            return 0;
        }
    }
}
