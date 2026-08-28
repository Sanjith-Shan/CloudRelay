package com.cloudrelay.service;

import com.cloudrelay.dto.CreateSessionRequest;
import com.cloudrelay.dto.JoinSessionRequest;
import com.cloudrelay.dto.MatchRequest;
import com.cloudrelay.dto.SessionResponse;
import com.cloudrelay.matchmaking.ExpandingWindow;
import com.cloudrelay.matchmaking.InMemoryWaitingPlayerIndex;
import com.cloudrelay.matchmaking.SkillMatchmaker;
import com.cloudrelay.matchmaking.WaitingPlayer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Instant;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The seam between matchmaking and the session lifecycle.
 *
 * <p>The algorithm is tested on its own in {@code SkillMatchmakerTest}. What is
 * tested here is that a formed party becomes a real session the same way the
 * FIFO path does — the anchor creates it, everybody else joins — and that the
 * metrics an operator tunes against actually get recorded.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class SkillMatchmakingServiceTest {

    private static final String GAME = "apex-legends";
    private static final String REGION = "us-west-2";

    @Mock
    private SessionService sessionService;

    private MeterRegistry meterRegistry;
    private InMemoryWaitingPlayerIndex index;
    private SkillMatchmakingService service;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        index = new InMemoryWaitingPlayerIndex();
        service = new SkillMatchmakingService(
                new SkillMatchmaker(index, new ExpandingWindow(100, 25, 600)),
                sessionService,
                meterRegistry);
    }

    private MatchRequest request(String playerId, Integer rating) {
        return MatchRequest.builder()
                .playerId(playerId).displayName("Player " + playerId)
                .gameId(GAME).region(REGION).skillRating(rating).build();
    }

    private SessionResponse session() {
        return SessionResponse.builder().sessionCode("PARTY001")
                .gameId(GAME).region(REGION).build();
    }

    @Test
    void enqueueReturnsTheQueueDepth() {
        assertThat(service.enqueue(request("a", 1200))).isEqualTo(1);
        assertThat(service.enqueue(request("b", 1210))).isEqualTo(2);
    }

    @Test
    void anUnratedPlayerIsQueuedAtTheDefaultRating() {
        service.enqueue(request("unrated", null));

        WaitingPlayer queued = index.longestWaiting(GAME, REGION).orElseThrow();
        assertThat(queued.rating()).isEqualTo(WaitingPlayer.DEFAULT_RATING);
    }

    @Test
    void anEmptyQueueMatchesNothingAndCreatesNoSession() {
        assertThat(service.tryMatch(GAME, REGION, 2)).isEmpty();
        verify(sessionService, never()).createSession(any());
    }

    @Test
    void aFormedPartyBecomesASessionHostedByTheAnchor() {
        when(sessionService.createSession(any(CreateSessionRequest.class))).thenReturn(session());
        when(sessionService.joinSession(anyString(), any(JoinSessionRequest.class)))
                .thenReturn(session());

        service.enqueue(request("anchor", 1500));
        service.enqueue(request("second", 1510));

        Optional<SessionResponse> result = service.tryMatch(GAME, REGION, 2);

        assertThat(result).isPresent();

        ArgumentCaptor<CreateSessionRequest> created =
                ArgumentCaptor.forClass(CreateSessionRequest.class);
        verify(sessionService).createSession(created.capture());
        assertThat(created.getValue().getPlayerId())
                .as("the longest waiting player hosts")
                .isEqualTo("anchor");
        assertThat(created.getValue().getGameId()).isEqualTo(GAME);

        ArgumentCaptor<JoinSessionRequest> joined =
                ArgumentCaptor.forClass(JoinSessionRequest.class);
        verify(sessionService).joinSession(anyString(), joined.capture());
        assertThat(joined.getValue().getPlayerId()).isEqualTo("second");
    }

    @Test
    void everyNonAnchorMemberJoinsTheSession() {
        when(sessionService.createSession(any(CreateSessionRequest.class))).thenReturn(session());
        when(sessionService.joinSession(anyString(), any(JoinSessionRequest.class)))
                .thenReturn(session());

        for (String id : new String[]{"a", "b", "c", "d"}) {
            service.enqueue(request(id, 1500));
        }

        service.tryMatch(GAME, REGION, 4);

        verify(sessionService, times(1)).createSession(any());
        verify(sessionService, times(3)).joinSession(anyString(), any());
    }

    @Test
    void matchQualityAndWaitAreBothRecorded() {
        when(sessionService.createSession(any(CreateSessionRequest.class))).thenReturn(session());
        when(sessionService.joinSession(anyString(), any(JoinSessionRequest.class)))
                .thenReturn(session());

        // Enqueued directly so the anchor has a known, non-zero wait.
        index.add(GAME, REGION, new WaitingPlayer(
                "anchor", "Anchor", REGION, 1500, Instant.now().minusSeconds(10)));
        index.add(GAME, REGION, new WaitingPlayer(
                "second", "Second", REGION, 1560, Instant.now()));

        service.tryMatch(GAME, REGION, 2);

        assertThat(meterRegistry.get("cloudrelay.matchmaking.parties.formed")
                .counter().count()).isEqualTo(1);
        assertThat(meterRegistry.get("cloudrelay.matchmaking.rating.spread")
                .summary().max()).isEqualTo(60);
        assertThat(meterRegistry.get("cloudrelay.matchmaking.wait.seconds")
                .summary().max()).isGreaterThanOrEqualTo(10);
    }

    @Test
    void anAttemptThatFindsNobodyIsCountedAndLeavesTheQueueIntact() {
        service.enqueue(request("lonely", 1200));

        assertThat(service.tryMatch(GAME, REGION, 4)).isEmpty();

        assertThat(meterRegistry.get("cloudrelay.matchmaking.attempts.unfilled")
                .counter().count()).isEqualTo(1);
        assertThat(service.queueDepth(GAME, REGION))
                .as("a failed attempt must not consume the queue")
                .isEqualTo(1);
        verify(sessionService, never()).createSession(any());
    }
}
