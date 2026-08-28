package com.cloudrelay.analytics;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * Reports whether the analytics tee is switched on.
 *
 * <p>Always UP. A deployment with no lakehouse attached is a supported
 * configuration, not a degraded one, and reporting DOWN for it would take
 * instances out of the load balancer over a dashboard. The detail is there so
 * that "why is the lakehouse empty" has a one request answer.
 */
@Component
@RequiredArgsConstructor
public class AnalyticsHealthIndicator implements HealthIndicator {

    private final AnalyticsEventSink sink;

    @Override
    public Health health() {
        return Health.up()
                .withDetail("tee", sink.isEnabled() ? "kafka" : "disabled")
                .build();
    }
}
