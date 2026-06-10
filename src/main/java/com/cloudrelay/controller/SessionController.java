package com.cloudrelay.controller;

import com.cloudrelay.dto.CreateSessionRequest;
import com.cloudrelay.dto.JoinSessionRequest;
import com.cloudrelay.dto.SessionResponse;
import com.cloudrelay.service.SessionService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.util.List;

@CrossOrigin
@RestController
@RequestMapping("/api/v1/sessions")
@RequiredArgsConstructor
public class SessionController {

    private final SessionService sessionService;

    @PostMapping
    public ResponseEntity<SessionResponse> createSession(
            @Valid @RequestBody CreateSessionRequest request) {
        SessionResponse response = sessionService.createSession(request);
        URI location = URI.create("/api/v1/sessions/" + response.getSessionCode());
        return ResponseEntity.created(location).body(response);
    }

    @GetMapping("/{sessionCode}")
    public ResponseEntity<SessionResponse> getSession(@PathVariable String sessionCode) {
        return ResponseEntity.ok(sessionService.getSession(sessionCode));
    }

    @PostMapping("/{sessionCode}/join")
    public ResponseEntity<SessionResponse> joinSession(
            @PathVariable String sessionCode,
            @Valid @RequestBody JoinSessionRequest request) {
        return ResponseEntity.ok(sessionService.joinSession(sessionCode, request));
    }

    @PostMapping("/{sessionCode}/leave")
    public ResponseEntity<SessionResponse> leaveSession(
            @PathVariable String sessionCode,
            @RequestParam String playerId) {
        return ResponseEntity.ok(sessionService.leaveSession(sessionCode, playerId));
    }

    @PostMapping("/{sessionCode}/start")
    public ResponseEntity<SessionResponse> startSession(
            @PathVariable String sessionCode,
            @RequestParam String playerId) {
        return ResponseEntity.ok(sessionService.startSession(sessionCode, playerId));
    }

    @DeleteMapping("/{sessionCode}")
    public ResponseEntity<SessionResponse> terminateSession(
            @PathVariable String sessionCode,
            @RequestParam String playerId) {
        return ResponseEntity.ok(sessionService.terminateSession(sessionCode, playerId));
    }

    @GetMapping
    public ResponseEntity<List<SessionResponse>> listActiveSessions(
            @RequestParam String gameId,
            @RequestParam String region) {
        return ResponseEntity.ok(sessionService.listActiveSessions(gameId, region));
    }

    @GetMapping("/player/{playerId}")
    public ResponseEntity<List<SessionResponse>> getPlayerSessions(
            @PathVariable String playerId) {
        return ResponseEntity.ok(sessionService.getPlayerSessions(playerId));
    }
}
