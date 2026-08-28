package com.cloudrelay.controller;

import com.cloudrelay.dto.MatchRequest;
import com.cloudrelay.dto.SessionResponse;
import com.cloudrelay.matchmaking.ExpandingWindow;
import com.cloudrelay.matchmaking.SkillMatchmaker;
import com.cloudrelay.model.SessionState;
import com.cloudrelay.service.MatchmakingService;
import com.cloudrelay.service.SkillMatchmakingService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(MatchmakingController.class)
class MatchmakingControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private MatchmakingService matchmakingService;

    @MockBean
    private SkillMatchmakingService skillMatchmakingService;

    @MockBean
    private SkillMatchmaker skillMatchmaker;

    @BeforeEach
    void setUp() {
        when(skillMatchmakingService.matcher()).thenReturn(skillMatchmaker);
        when(skillMatchmaker.window()).thenReturn(new ExpandingWindow(100, 25, 600));
    }

    private SessionResponse session() {
        return SessionResponse.builder()
                .sessionCode("ABCD1234")
                .gameId("fortnite")
                .region("us-east-1")
                .state(SessionState.WAITING)
                .currentPlayerCount(2)
                .maxPlayers(4)
                .build();
    }

    private MatchRequest request(Integer rating) {
        return MatchRequest.builder()
                .playerId("player-1")
                .displayName("Alice")
                .gameId("fortnite")
                .region("us-east-1")
                .skillRating(rating)
                .build();
    }

    // ---- FIFO ------------------------------------------------------------

    @Test
    void enqueue_returns202WithQueuePosition() throws Exception {
        when(matchmakingService.enqueue(any())).thenReturn("Player player-1 queued at position 3");
        when(matchmakingService.getQueueDepth("fortnite", "us-east-1")).thenReturn(3L);

        mockMvc.perform(post("/api/v1/matchmaking/enqueue")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request(null))))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.queuePosition").value(3));
    }

    @Test
    void enqueue_rejectsAMissingGameId() throws Exception {
        mockMvc.perform(post("/api/v1/matchmaking/enqueue")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"playerId\":\"p1\",\"displayName\":\"A\",\"region\":\"us-east-1\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.fields.gameId").exists());
    }

    @Test
    void match_returnsTheSession() throws Exception {
        when(matchmakingService.tryMatch("fortnite", "us-east-1"))
                .thenReturn(Optional.of(session()));

        mockMvc.perform(post("/api/v1/matchmaking/match")
                        .param("gameId", "fortnite").param("region", "us-east-1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.sessionCode").value("ABCD1234"));
    }

    @Test
    void match_emptyQueueReturns204() throws Exception {
        when(matchmakingService.tryMatch("fortnite", "us-east-1")).thenReturn(Optional.empty());

        mockMvc.perform(post("/api/v1/matchmaking/match")
                        .param("gameId", "fortnite").param("region", "us-east-1"))
                .andExpect(status().isNoContent());
    }

    // ---- Skill based -----------------------------------------------------

    @Test
    void skillEnqueue_returns202WithTheRatingAndWindowEstimate() throws Exception {
        when(skillMatchmakingService.enqueue(any())).thenReturn(5L);

        mockMvc.perform(post("/api/v1/matchmaking/skill/enqueue")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request(1450))))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.rating").value(1450))
                .andExpect(jsonPath("$.queueDepth").value(5))
                // The client can show an honest wait estimate rather than a
                // spinner: this is when the search reaches its widest.
                .andExpect(jsonPath("$.secondsToWidestSearch").value(20));
    }

    @Test
    void skillEnqueue_anUnratedPlayerIsTreatedAsAverageNotAsZero() throws Exception {
        when(skillMatchmakingService.enqueue(any())).thenReturn(1L);

        mockMvc.perform(post("/api/v1/matchmaking/skill/enqueue")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request(null))))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.rating").value(1200));
    }

    @Test
    void skillEnqueue_rejectsANonsenseRating() throws Exception {
        mockMvc.perform(post("/api/v1/matchmaking/skill/enqueue")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request(99_999))))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.fields.skillRating").exists());
    }

    @Test
    void skillMatch_returnsTheAssembledParty() throws Exception {
        when(skillMatchmakingService.tryMatch(eq("fortnite"), eq("us-east-1"), anyInt()))
                .thenReturn(Optional.of(session()));

        mockMvc.perform(post("/api/v1/matchmaking/skill/match")
                        .param("gameId", "fortnite").param("region", "us-east-1")
                        .param("partySize", "4"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.sessionCode").value("ABCD1234"));
    }

    @Test
    void skillMatch_tooFewCompatiblePlayersReturns204NotAnError() throws Exception {
        // Not enough compatible players is the normal state of a queue most of
        // the time, not a failure. The client polls and the windows widen.
        when(skillMatchmakingService.tryMatch(eq("fortnite"), eq("us-east-1"), anyInt()))
                .thenReturn(Optional.empty());

        mockMvc.perform(post("/api/v1/matchmaking/skill/match")
                        .param("gameId", "fortnite").param("region", "us-east-1"))
                .andExpect(status().isNoContent());
    }

    @Test
    void skillQueue_reportsDepthAndTheWindowPolicy() throws Exception {
        when(skillMatchmakingService.queueDepth("fortnite", "us-east-1")).thenReturn(12L);

        mockMvc.perform(get("/api/v1/matchmaking/skill/queue")
                        .param("gameId", "fortnite").param("region", "us-east-1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.depth").value(12))
                .andExpect(jsonPath("$.initialWindow").value(100))
                .andExpect(jsonPath("$.maxWindow").value(600));
    }
}
