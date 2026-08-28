package com.cloudrelay.analytics;

import com.cloudrelay.event.SessionEvent;

/** The no-op sink used when no analytics pipeline is configured. */
public final class DisabledAnalyticsSink implements AnalyticsEventSink {

    @Override
    public void accept(SessionEvent event) {
        // Intentionally nothing.
    }

    @Override
    public boolean isEnabled() {
        return false;
    }
}
