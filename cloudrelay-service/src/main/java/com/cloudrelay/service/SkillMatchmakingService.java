package com.cloudrelay.service;

import com.cloudrelay.dto.CreateSessionRequest;
import com.cloudrelay.dto.JoinSessionRequest;
import com.cloudrelay.dto.MatchRequest;
import com.cloudrelay.dto.SessionResponse;
import com.cloudrelay.matchmaking.Party;
import com.cloudrelay.matchmaking.SkillMatchmaker;
import com.cloudrelay.matchmaking.WaitingPlayer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Skill based matchmaking, sitting alongside the original FIFO matcher rather
 * than replacing it.
 *
 * <p>Both are kept deliberately. They are the two ends of the trade every
 * matchmaker makes — fill the lobby fastest, or fill it with the right people —
 * and having both behind one API is what makes the comparison in
 * {@code MatchmakingBenchmarkTest} an honest measurement rather than a claim.
 * A game with a thin player base is genuinely better served by FIFO; one with a
 * dense queue is not.
 *
 * <p>The party is turned into a session by reusing {@link SessionService}
 * exactly as the FIFO path does: the anchor creates the session and everybody
 * else joins it. Matchmaking decides who plays together and stops there; the
 * session state machine, the optimistic locking and the event publishing are
 * already solved one layer down and are not worth a second implementation.
 */
@Slf4j
@Service
public class SkillMatchmakingService {

    private final SkillMatchmaker matchmaker;
    private final SessionService sessionService;

    private final Counter partiesFormed;
    private final Counter matchAttemptsUnfilled;
    private final DistributionSummary ratingSpread;
    private final DistributionSummary anchorWaitSeconds;

    public SkillMatchmakingService(SkillMatchmaker matchmaker,
                                   SessionService sessionService,
                                   MeterRegistry meterRegistry) {
        this.matchmaker = matchmaker;
        this.sessionService = sessionService;

        this.partiesFormed = Counter.builder("cloudrelay.matchmaking.parties.formed")
                .description("Parties assembled by the skill based matcher")
                .register(meterRegistry);
        this.matchAttemptsUnfilled = Counter.builder("cloudrelay.matchmaking.attempts.unfilled")
                .description("Match attempts that found too few compatible players")
                .register(meterRegistry);
        // The two numbers that say whether matchmaking is doing its job. Spread
        // is match quality, wait is the price paid for it, and a change to the
        // window policy moves both. Watching one without the other is how a
        // matchmaker gets tuned into producing fast, terrible games.
        this.ratingSpread = DistributionSummary.builder("cloudrelay.matchmaking.rating.spread")
                .description("Rating gap between the best and worst player in a formed party")
                .baseUnit("rating points")
                .publishPercentileHistogram()
                // Bucket bounds matter here. Micrometer's default percentile
                // histogram is shaped for latencies in the millisecond range,
                // and on a value that runs from 0 to a few hundred rating
                // points it would put almost every observation in one bucket
                // and make the percentiles meaningless. The window caps at 600
                // points, so a party can span at most twice that.
                .minimumExpectedValue(1.0)
                .maximumExpectedValue(1200.0)
                .register(meterRegistry);
        this.anchorWaitSeconds = DistributionSummary.builder("cloudrelay.matchmaking.wait.seconds")
                .description("How long the longest waiting player in a party had queued")
                .baseUnit("seconds")
                .publishPercentileHistogram()
                // A wait beyond a few minutes means the game has no players,
                // not that matchmaking is slow, so there is nothing to gain
                // from resolution past 300 seconds.
                .minimumExpectedValue(1.0)
                .maximumExpectedValue(300.0)
                .register(meterRegistry);
    }

    public long enqueue(MatchRequest request) {
        WaitingPlayer player = new WaitingPlayer(
                request.getPlayerId(),
                request.getDisplayName(),
                request.getRegion(),
                request.getSkillRating() == null
                        ? WaitingPlayer.DEFAULT_RATING
                        : request.getSkillRating(),
                Instant.now());

        matchmaker.enqueue(request.getGameId(), request.getRegion(), player);
        long depth = matchmaker.queueDepth(request.getGameId(), request.getRegion());
        log.info("Player {} queued for skill match in {} {} at rating {}, queue depth {}",
                player.playerId(), request.getGameId(), request.getRegion(),
                player.rating(), depth);
        return depth;
    }

    /**
     * Attempts one match. Returns empty when there are not yet enough
     * compatible players, which is a normal outcome and not an error: the
     * waiting players' windows are wider on the next attempt.
     */
    public Optional<SessionResponse> tryMatch(String gameId, String region, int partySize) {
        Optional<Party> maybeParty =
                matchmaker.formParty(gameId, region, partySize, Instant.now());

        if (maybeParty.isEmpty()) {
            matchAttemptsUnfilled.increment();
            return Optional.empty();
        }

        Party party = maybeParty.get();
        partiesFormed.increment();
        ratingSpread.record(party.ratingSpread());
        anchorWaitSeconds.record(party.anchorWait().getSeconds());

        WaitingPlayer anchor = party.anchor();
        SessionResponse session = sessionService.createSession(CreateSessionRequest.builder()
                .gameId(gameId)
                .playerId(anchor.playerId())
                .displayName(anchor.displayName())
                .region(region)
                .maxPlayers(Math.max(partySize, 2))
                .minPlayersToStart(Math.max(Math.min(partySize, 2), 2))
                .build());

        List<WaitingPlayer> others = party.members().subList(1, party.size());
        for (WaitingPlayer member : others) {
            session = sessionService.joinSession(session.getSessionCode(),
                    JoinSessionRequest.builder()
                            .playerId(member.playerId())
                            .displayName(member.displayName())
                            .region(member.region())
                            .build());
        }

        log.info("Formed party of {} in session {} with a rating spread of {} "
                        + "after the anchor waited {}s",
                party.size(), session.getSessionCode(), party.ratingSpread(),
                party.anchorWait().getSeconds());
        return Optional.of(session);
    }

    public long queueDepth(String gameId, String region) {
        return matchmaker.queueDepth(gameId, region);
    }

    /** The current window policy, so clients can show an honest wait estimate. */
    public SkillMatchmaker matcher() {
        return matchmaker;
    }
}
