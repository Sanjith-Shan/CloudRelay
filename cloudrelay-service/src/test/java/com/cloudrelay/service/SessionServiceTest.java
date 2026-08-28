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
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class SessionServiceTest {

    @Mock
    private SessionRepository sessionRepository;

    @Mock
    private RedisTemplate<String, Object> redisTemplate;

    @Mock
    private ValueOperations<String, Object> valueOperations;

    @Mock
    private SessionEventPublisher eventPublisher;

    private SessionService sessionService;

    @BeforeEach
    void setUp() {
        lenient().when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        sessionService = new SessionService(
                sessionRepository, redisTemplate, new SimpleMeterRegistry(),
                eventPublisher, 8, 2, 6);
    }

    private CreateSessionRequest createRequest() {
        return CreateSessionRequest.builder()
                .gameId("cyberpunk-2077")
                .playerId("player-1")
                .displayName("Host Player")
                .region("us-west-2")
                .maxPlayers(4)
                .minPlayersToStart(2)
                .build();
    }

    private GameSession sessionWithPlayers(int count, int maxPlayers, SessionState state) {
        List<Player> players = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            players.add(Player.builder()
                    .playerId("player-" + i)
                    .displayName("Player " + i)
                    .region("us-west-2")
                    .joinedAt(Instant.now())
                    .connected(true)
                    .build());
        }
        return GameSession.builder()
                .id("session-id")
                .sessionCode("ABCD1234")
                .gameId("cyberpunk-2077")
                .region("us-west-2")
                .state(state)
                .host(players.isEmpty() ? null : players.get(0))
                .players(players)
                .maxPlayers(maxPlayers)
                .minPlayersToStart(2)
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();
    }

    @Test
    void createSession_generatesUniqueCode() {
        when(sessionRepository.findBySessionCode(anyString())).thenReturn(Optional.empty());
        when(sessionRepository.save(any(GameSession.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        SessionResponse response = sessionService.createSession(createRequest());

        assertThat(response.getSessionCode()).isNotNull();
        assertThat(response.getSessionCode()).hasSize(8);
        assertThat(response.getSessionCode()).matches("[A-Z0-9]{8}");
    }

    @Test
    void createSession_setsInitialState() {
        when(sessionRepository.findBySessionCode(anyString())).thenReturn(Optional.empty());
        when(sessionRepository.save(any(GameSession.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        SessionResponse response = sessionService.createSession(createRequest());

        assertThat(response.getState()).isEqualTo(SessionState.WAITING);
        assertThat(response.getCurrentPlayerCount()).isEqualTo(1);
        assertThat(response.getHost().getPlayerId()).isEqualTo("player-1");
        verify(eventPublisher).publish(any(), any(SessionResponse.class));
    }

    @Test
    void getSession_cacheHit_doesNotQueryMongo() {
        GameSession cached = sessionWithPlayers(2, 4, SessionState.WAITING);
        when(valueOperations.get("session:ABCD1234")).thenReturn(cached);

        SessionResponse response = sessionService.getSession("ABCD1234");

        assertThat(response.getSessionCode()).isEqualTo("ABCD1234");
        verify(sessionRepository, never()).findBySessionCode(anyString());
    }

    @Test
    void getSession_cacheMiss_queriesMongo() {
        GameSession stored = sessionWithPlayers(2, 4, SessionState.WAITING);
        when(valueOperations.get("session:ABCD1234")).thenReturn(null);
        when(sessionRepository.findBySessionCode("ABCD1234")).thenReturn(Optional.of(stored));

        SessionResponse response = sessionService.getSession("ABCD1234");

        assertThat(response.getSessionCode()).isEqualTo("ABCD1234");
        verify(sessionRepository).findBySessionCode("ABCD1234");
        verify(valueOperations).set(any(), any());
    }

    @Test
    void getSession_notFound_throwsException() {
        when(valueOperations.get(anyString())).thenReturn(null);
        when(sessionRepository.findBySessionCode("MISSING1")).thenReturn(Optional.empty());

        assertThatThrownBy(() -> sessionService.getSession("MISSING1"))
                .isInstanceOf(SessionNotFoundException.class);
    }

    @Test
    void joinSession_addsPlayer() {
        GameSession session = sessionWithPlayers(1, 4, SessionState.WAITING);
        session.setMinPlayersToStart(3);
        when(valueOperations.get("session:ABCD1234")).thenReturn(session);
        when(sessionRepository.save(any(GameSession.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        JoinSessionRequest request = JoinSessionRequest.builder()
                .playerId("player-9").displayName("Joiner").region("us-west-2").build();
        SessionResponse response = sessionService.joinSession("ABCD1234", request);

        assertThat(response.getCurrentPlayerCount()).isEqualTo(2);
        assertThat(response.getPlayers())
                .extracting(Player::getPlayerId)
                .contains("player-9");
    }

    @Test
    void joinSession_fullSession_throwsException() {
        GameSession session = sessionWithPlayers(4, 4, SessionState.WAITING);
        when(valueOperations.get("session:ABCD1234")).thenReturn(session);

        JoinSessionRequest request = JoinSessionRequest.builder()
                .playerId("player-9").displayName("Joiner").region("us-west-2").build();

        assertThatThrownBy(() -> sessionService.joinSession("ABCD1234", request))
                .isInstanceOf(SessionFullException.class);
        verify(sessionRepository, never()).save(any());
    }

    @Test
    void joinSession_duplicatePlayer_throwsException() {
        GameSession session = sessionWithPlayers(2, 4, SessionState.WAITING);
        when(valueOperations.get("session:ABCD1234")).thenReturn(session);

        JoinSessionRequest request = JoinSessionRequest.builder()
                .playerId("player-1").displayName("Duplicate").region("us-west-2").build();

        assertThatThrownBy(() -> sessionService.joinSession("ABCD1234", request))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void joinSession_reachesMinPlayers_transitionsToStarting() {
        GameSession session = sessionWithPlayers(1, 4, SessionState.WAITING);
        when(valueOperations.get("session:ABCD1234")).thenReturn(session);
        when(sessionRepository.save(any(GameSession.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        JoinSessionRequest request = JoinSessionRequest.builder()
                .playerId("player-2").displayName("Second").region("us-west-2").build();
        SessionResponse response = sessionService.joinSession("ABCD1234", request);

        assertThat(response.getState()).isEqualTo(SessionState.STARTING);
    }

    @Test
    void leaveSession_lastPlayer_terminates() {
        GameSession session = sessionWithPlayers(1, 4, SessionState.WAITING);
        when(valueOperations.get("session:ABCD1234")).thenReturn(session);
        when(sessionRepository.save(any(GameSession.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        SessionResponse response = sessionService.leaveSession("ABCD1234", "player-1");

        assertThat(response.getState()).isEqualTo(SessionState.TERMINATED);
        verify(redisTemplate).delete("session:ABCD1234");
    }

    @Test
    void leaveSession_hostLeaves_reassignsHost() {
        GameSession session = sessionWithPlayers(3, 4, SessionState.STARTING);
        when(valueOperations.get("session:ABCD1234")).thenReturn(session);
        when(sessionRepository.save(any(GameSession.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        SessionResponse response = sessionService.leaveSession("ABCD1234", "player-1");

        assertThat(response.getHost().getPlayerId()).isEqualTo("player-2");
        assertThat(response.getCurrentPlayerCount()).isEqualTo(2);
    }

    @Test
    void startSession_nonHost_throwsException() {
        GameSession session = sessionWithPlayers(2, 4, SessionState.STARTING);
        when(valueOperations.get("session:ABCD1234")).thenReturn(session);

        assertThatThrownBy(() -> sessionService.startSession("ABCD1234", "player-2"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void startSession_byHost_activatesAndExtendsExpiry() {
        GameSession session = sessionWithPlayers(2, 4, SessionState.STARTING);
        Instant before = Instant.now();
        when(valueOperations.get("session:ABCD1234")).thenReturn(session);
        when(sessionRepository.save(any(GameSession.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        SessionResponse response = sessionService.startSession("ABCD1234", "player-1");

        assertThat(response.getState()).isEqualTo(SessionState.ACTIVE);
        assertThat(response.getExpiresAt()).isAfter(before.plusSeconds(5 * 3600));
    }
}
