package com.cloudrelay.controller;

import com.cloudrelay.dto.MatchRequest;
import com.cloudrelay.matchmaking.WaitingPlayer;
import com.cloudrelay.dto.SessionResponse;
import com.cloudrelay.service.MatchmakingService;
import com.cloudrelay.service.SkillMatchmakingService;
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
    private final SkillMatchmakingService skillMatchmakingService;

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

    // ---- Skill based matchmaking -----------------------------------------
    //
    // A parallel set of endpoints rather than a flag on the ones above. The two
    // matchers have genuinely different semantics — FIFO matches one player at
    // a time into whatever session has room, skill matching assembles a whole
    // party at once — and hiding that behind a boolean would make the response
    // shape depend on a query parameter.

    @PostMapping("/skill/enqueue")
    public ResponseEntity<Map<String, Object>> enqueueForSkillMatch(
            @Valid @RequestBody MatchRequest request) {
        long depth = skillMatchmakingService.enqueue(request);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(Map.of(
                "playerId", request.getPlayerId(),
                "rating", request.getSkillRating() == null
                        ? WaitingPlayer.DEFAULT_RATING : request.getSkillRating(),
                "queueDepth", depth,
                "secondsToWidestSearch",
                skillMatchmakingService.matcher().window().timeToMaxWidth().getSeconds()));
    }

    /**
     * Attempts one party. 204 means there are not yet enough compatible players,
     * which is a normal state rather than a failure — the caller polls, and the
     * waiting players' search windows widen in the meantime.
     */
    @PostMapping("/skill/match")
    public ResponseEntity<SessionResponse> skillMatch(
            @RequestParam String gameId,
            @RequestParam String region,
            @RequestParam(defaultValue = "2") int partySize) {
        return skillMatchmakingService.tryMatch(gameId, region, partySize)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.noContent().build());
    }

    @GetMapping("/skill/queue")
    public ResponseEntity<Map<String, Object>> skillQueueDepth(
            @RequestParam String gameId,
            @RequestParam String region) {
        return ResponseEntity.ok(Map.of(
                "gameId", gameId,
                "region", region,
                "depth", skillMatchmakingService.queueDepth(gameId, region),
                "initialWindow", skillMatchmakingService.matcher().window().initialWidth(),
                "maxWindow", skillMatchmakingService.matcher().window().maxWidth()));
    }
}
