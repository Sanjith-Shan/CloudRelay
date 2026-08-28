package com.cloudrelay.config;

import com.cloudrelay.matchmaking.ExpandingWindow;
import com.cloudrelay.matchmaking.RedisWaitingPlayerIndex;
import com.cloudrelay.matchmaking.SkillMatchmaker;
import com.cloudrelay.matchmaking.WaitingPlayerIndex;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.RedisTemplate;

/**
 * Wires the skill based matchmaker.
 *
 * <p>The index is Redis backed so that replicas share one queue. Swapping in
 * {@code InMemoryWaitingPlayerIndex} here gives a single instance deployment
 * the same behaviour without Redis, which is exactly what the benchmark and the
 * correctness tests do.
 */
@Configuration
public class MatchmakingConfig {

    @Bean
    public WaitingPlayerIndex waitingPlayerIndex(RedisTemplate<String, Object> redisTemplate) {
        return new RedisWaitingPlayerIndex(redisTemplate);
    }

    @Bean
    public SkillMatchmaker skillMatchmaker(
            WaitingPlayerIndex index,
            @Value("${CLOUDRELAY_MATCHMAKING_INITIAL_WINDOW:100}") int initialWindow,
            @Value("${CLOUDRELAY_MATCHMAKING_WINDOW_GROWTH_PER_SECOND:25}") int growthPerSecond,
            @Value("${CLOUDRELAY_MATCHMAKING_MAX_WINDOW:600}") int maxWindow) {
        return new SkillMatchmaker(index,
                new ExpandingWindow(initialWindow, growthPerSecond, maxWindow));
    }
}
