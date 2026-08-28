package com.cloudrelay.analytics;

import com.cloudrelay.event.SessionEvent;

/**
 * Where lifecycle events go for analysis, as opposed to where they go to keep
 * clients in sync.
 *
 * <p>Behind an interface for a specific reason: the service must run with no
 * analytics pipeline at all. Local development, the unit tests and any
 * deployment that has not turned Kafka on all get {@link DisabledAnalyticsSink}
 * and behave identically. Making the sink optional at the type level rather
 * than with a null check means no call site has to remember.
 *
 * <p>Implementations must never throw and must never block the caller. The
 * caller is a player waiting on an HTTP response.
 */
public interface AnalyticsEventSink {

    void accept(SessionEvent event);

    /** Whether events are actually leaving the process. Reported on the health endpoint. */
    boolean isEnabled();
}
