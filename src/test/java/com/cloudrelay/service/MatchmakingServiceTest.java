package com.cloudrelay.service;

import com.cloudrelay.dto.CreateSessionRequest;
import com.cloudrelay.dto.JoinSessionRequest;
import com.cloudrelay.dto.MatchRequest;
import com.cloudrelay.dto.SessionResponse;
import com.cloudrelay.model.GameSession;
import com.cloudrelay.model.Player;
import com.cloudrelay.model.SessionState;
import com.cloudrelay.repository.SessionRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.RedisTemplate;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class MatchmakingServiceTest {

    private static final String QUEUE_KEY = "matchqueue:cyberpunk-2077:us-west-2";

    @Mock
    private SessionRepository sessionRepository;

    @Mock
    private RedisTemplate<String, Object> redisTemplate;

    @Mock
    private ListOperations<String, Object> listOperations;

    @Mock
    private SessionService sessionService;

    @InjectMocks
    private MatchmakingService matchmakingService;

    @BeforeEach
    void setUp() {
        lenient().when(redisTemplate.opsForList()).thenReturn(listOperations);
    }

    private MatchRequest matchRequest() {
        return MatchRequest.builder()
                .playerId("player-7")
                .displayName("Seeker")
                .gameId("cyberpunk-2077")
                .region("us-west-2")
                .build();
    }

    private GameSession openSession(int playerCount, int maxPlayers) {
        List<Player> players = new ArrayList<>();
        for (int i = 1; i <= playerCount; i++) {
            players.add(Player.builder()
                    .playerId("player-" + i)
                    .displayName("Player " + i)
                    .region("us-west-2")
                    .joinedAt(Instant.now())
                    .build());
        }
        return GameSession.builder()
                .sessionCode("OPEN5678")
                .gameId("cyberpunk-2077")
                .region("us-west-2")
                .state(SessionState.WAITING)
                .players(players)
                .maxPlayers(maxPlayers)
                .minPlayersToStart(2)
                .build();
    }

    @Test
    void enqueue_addsToRedisQueue() {
        when(listOperations.size(QUEUE_KEY)).thenReturn(3L);

        String message = matchmakingService.enqueue(matchRequest());

        verify(listOperations).leftPush(eq(QUEUE_KEY), any(MatchRequest.class));
        assertThat(message).contains("player-7").contains("3");
    }

    @Test
    void tryMatch_existingSession_joinsIt() {
        when(listOperations.rightPop(QUEUE_KEY)).thenReturn(matchRequest());
        when(sessionRepository.findByGameIdAndRegionAndState(
                "cyberpunk-2077", "us-west-2", SessionState.WAITING))
                .thenReturn(List.of(openSession(2, 4)));
        SessionResponse joined = SessionResponse.builder().sessionCode("OPEN5678").build();
        when(sessionService.joinSession(eq("OPEN5678"), any(JoinSessionRequest.class)))
                .thenReturn(joined);

        Optional<SessionResponse> result =
                matchmakingService.tryMatch("cyberpunk-2077", "us-west-2");

        assertThat(result).isPresent();
        assertThat(result.get().getSessionCode()).isEqualTo("OPEN5678");
        verify(sessionService, never()).createSession(any());
    }

    @Test
    void tryMatch_noSession_createsNew() {
        when(listOperations.rightPop(QUEUE_KEY)).thenReturn(matchRequest());
        when(sessionRepository.findByGameIdAndRegionAndState(
                "cyberpunk-2077", "us-west-2", SessionState.WAITING))
                .thenReturn(List.of());
        SessionResponse created = SessionResponse.builder().sessionCode("NEWW9012").build();
        when(sessionService.createSession(any(CreateSessionRequest.class))).thenReturn(created);

        Optional<SessionResponse> result =
                matchmakingService.tryMatch("cyberpunk-2077", "us-west-2");

        assertThat(result).isPresent();
        assertThat(result.get().getSessionCode()).isEqualTo("NEWW9012");
        verify(sessionService, never()).joinSession(any(), any());
    }

    @Test
    void tryMatch_fullSessionsOnly_createsNew() {
        when(listOperations.rightPop(QUEUE_KEY)).thenReturn(matchRequest());
        when(sessionRepository.findByGameIdAndRegionAndState(
                "cyberpunk-2077", "us-west-2", SessionState.WAITING))
                .thenReturn(List.of(openSession(4, 4)));
        SessionResponse created = SessionResponse.builder().sessionCode("NEWW9012").build();
        when(sessionService.createSession(any(CreateSessionRequest.class))).thenReturn(created);

        Optional<SessionResponse> result =
                matchmakingService.tryMatch("cyberpunk-2077", "us-west-2");

        assertThat(result).isPresent();
        verify(sessionService, never()).joinSession(any(), any());
    }

    @Test
    void tryMatch_emptyQueue_returnsEmpty() {
        when(listOperations.rightPop(QUEUE_KEY)).thenReturn(null);

        Optional<SessionResponse> result =
                matchmakingService.tryMatch("cyberpunk-2077", "us-west-2");

        assertThat(result).isEmpty();
        verify(sessionService, never()).joinSession(any(), any());
        verify(sessionService, never()).createSession(any());
    }

    @Test
    void getQueueDepth_returnsCorrectCount() {
        when(listOperations.size(QUEUE_KEY)).thenReturn(5L);

        long depth = matchmakingService.getQueueDepth("cyberpunk-2077", "us-west-2");

        assertThat(depth).isEqualTo(5);
    }
}
