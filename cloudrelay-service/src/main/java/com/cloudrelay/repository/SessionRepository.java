package com.cloudrelay.repository;

import com.cloudrelay.model.GameSession;
import com.cloudrelay.model.SessionState;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface SessionRepository extends MongoRepository<GameSession, String> {

    Optional<GameSession> findBySessionCode(String sessionCode);

    List<GameSession> findByGameIdAndRegionAndState(String gameId, String region, SessionState state);

    List<GameSession> findByStateIn(List<SessionState> states);

    long countByGameIdAndState(String gameId, SessionState state);

    List<GameSession> findByStateAndPlayersPlayerId(SessionState state, String playerId);
}
