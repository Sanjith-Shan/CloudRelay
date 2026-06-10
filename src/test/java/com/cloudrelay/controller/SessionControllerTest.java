package com.cloudrelay.controller;

import com.cloudrelay.dto.CreateSessionRequest;
import com.cloudrelay.dto.JoinSessionRequest;
import com.cloudrelay.dto.SessionResponse;
import com.cloudrelay.exception.SessionFullException;
import com.cloudrelay.exception.SessionNotFoundException;
import com.cloudrelay.model.Player;
import com.cloudrelay.model.SessionState;
import com.cloudrelay.service.SessionService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(SessionController.class)
class SessionControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private SessionService sessionService;

    private SessionResponse sampleResponse() {
        Player host = Player.builder()
                .playerId("player-1")
                .displayName("Host Player")
                .region("us-west-2")
                .joinedAt(Instant.now())
                .connected(true)
                .build();
        return SessionResponse.builder()
                .id("session-id")
                .sessionCode("ABCD1234")
                .gameId("cyberpunk-2077")
                .region("us-west-2")
                .state(SessionState.WAITING)
                .host(host)
                .players(List.of(host))
                .maxPlayers(4)
                .minPlayersToStart(2)
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .currentPlayerCount(1)
                .isFull(false)
                .uptimeSeconds(0)
                .build();
    }

    @Test
    void createSession_validRequest_returns201() throws Exception {
        when(sessionService.createSession(any(CreateSessionRequest.class)))
                .thenReturn(sampleResponse());

        CreateSessionRequest request = CreateSessionRequest.builder()
                .gameId("cyberpunk-2077")
                .playerId("player-1")
                .displayName("Host Player")
                .region("us-west-2")
                .build();

        mockMvc.perform(post("/api/v1/sessions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isCreated())
                .andExpect(header().string("Location", "/api/v1/sessions/ABCD1234"))
                .andExpect(jsonPath("$.sessionCode").value("ABCD1234"))
                .andExpect(jsonPath("$.state").value("WAITING"));
    }

    @Test
    void createSession_invalidRequest_returns400() throws Exception {
        mockMvc.perform(post("/api/v1/sessions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("Validation failed"))
                .andExpect(jsonPath("$.fields.gameId").exists());
    }

    @Test
    void getSession_exists_returns200() throws Exception {
        when(sessionService.getSession("ABCD1234")).thenReturn(sampleResponse());

        mockMvc.perform(get("/api/v1/sessions/ABCD1234"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.sessionCode").value("ABCD1234"))
                .andExpect(jsonPath("$.gameId").value("cyberpunk-2077"))
                .andExpect(jsonPath("$.currentPlayerCount").value(1));
    }

    @Test
    void getSession_notFound_returns404() throws Exception {
        when(sessionService.getSession("MISSING1"))
                .thenThrow(new SessionNotFoundException("MISSING1"));

        mockMvc.perform(get("/api/v1/sessions/MISSING1"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error").value("Session not found"))
                .andExpect(jsonPath("$.sessionCode").value("MISSING1"))
                .andExpect(jsonPath("$.timestamp").exists());
    }

    @Test
    void joinSession_success_returns200() throws Exception {
        when(sessionService.joinSession(eq("ABCD1234"), any(JoinSessionRequest.class)))
                .thenReturn(sampleResponse());

        JoinSessionRequest request = JoinSessionRequest.builder()
                .playerId("player-2")
                .displayName("Second Player")
                .region("us-west-2")
                .build();

        mockMvc.perform(post("/api/v1/sessions/ABCD1234/join")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.sessionCode").value("ABCD1234"));
    }

    @Test
    void joinSession_full_returns409() throws Exception {
        when(sessionService.joinSession(eq("ABCD1234"), any(JoinSessionRequest.class)))
                .thenThrow(new SessionFullException("ABCD1234", 4));

        JoinSessionRequest request = JoinSessionRequest.builder()
                .playerId("player-5")
                .displayName("Late Player")
                .region("us-west-2")
                .build();

        mockMvc.perform(post("/api/v1/sessions/ABCD1234/join")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.error").value("Session is full"))
                .andExpect(jsonPath("$.maxPlayers").value(4));
    }
}
