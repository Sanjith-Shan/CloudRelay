package com.cloudrelay.controller;

import com.cloudrelay.dto.MatchRequest;
import com.cloudrelay.dto.SessionResponse;
import com.cloudrelay.service.MatchmakingService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@CrossOrigin
@RestController
@RequestMapping("/api/v1/matchmaking")
@RequiredArgsConstructor
public class MatchmakingController {

    private final MatchmakingService matchmakingService;

    @PostMapping("/enqueue")
    public ResponseEntity<Map<String, Object>> enqueue(
            @Valid @RequestBody MatchRequest request) {
        String message = matchmakingService.enqueue(request);
        long position = matchmakingService.getQueueDepth(request.getGameId(), request.getRegion());
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(Map.of("message", message, "queuePosition", position));
    }

    @PostMapping("/match")
    public ResponseEntity<SessionResponse> match(
            @RequestParam String gameId,
            @RequestParam String region) {
        return matchmakingService.tryMatch(gameId, region)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.noContent().build());
    }

    @GetMapping("/queue")
    public ResponseEntity<Map<String, Object>> queueDepth(
            @RequestParam String gameId,
            @RequestParam String region) {
        long depth = matchmakingService.getQueueDepth(gameId, region);
        return ResponseEntity.ok(Map.of("gameId", gameId, "region", region, "depth", depth));
    }
}
