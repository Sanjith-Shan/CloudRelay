package com.cloudrelay.matchmaking;

import java.time.Duration;
import java.util.List;

/**
 * A group of players the matchmaker decided should play together.
 *
 * <p>{@link #ratingSpread()} is the number that says whether the match is any
 * good: the gap between the strongest and weakest player in it. A matchmaker is
 * judged on the distribution of this across many parties, not on how fast it
 * runs, and it is reported here so that judgement is possible.
 */
public record Party(List<WaitingPlayer> members, Duration anchorWait) {

    /** Rating gap between the best and worst player. Lower is a better match. */
    public int ratingSpread() {
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        for (WaitingPlayer p : members) {
            min = Math.min(min, p.rating());
            max = Math.max(max, p.rating());
        }
        return members.isEmpty() ? 0 : max - min;
    }

    public int size() {
        return members.size();
    }

    /** The player the party was built around, always the longest waiting one. */
    public WaitingPlayer anchor() {
        return members.get(0);
    }
}
