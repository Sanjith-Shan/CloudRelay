package com.cloudrelay.matchmaking;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ExpandingWindowTest {

    private final ExpandingWindow window = new ExpandingWindow(100, 25, 600);

    @Test
    void freshPlayerGetsTheNarrowWindow() {
        assertThat(window.widthAfter(Duration.ZERO)).isEqualTo(100);
    }

    @Test
    void windowGrowsLinearlyWithWaiting() {
        assertThat(window.widthAfter(Duration.ofSeconds(4))).isEqualTo(200);
        assertThat(window.widthAfter(Duration.ofSeconds(8))).isEqualTo(300);
    }

    @Test
    void windowStopsAtTheCap() {
        assertThat(window.widthAfter(Duration.ofSeconds(20))).isEqualTo(600);
        assertThat(window.widthAfter(Duration.ofHours(1))).isEqualTo(600);
    }

    @Test
    void negativeWaitIsTreatedAsNoWait() {
        // Clock skew between replicas can produce an enqueue time in the
        // future. It must not produce a negative window.
        assertThat(window.widthAfter(Duration.ofSeconds(-30))).isEqualTo(100);
    }

    @Test
    void reportsHowLongUntilTheWindowIsWidest() {
        assertThat(window.timeToMaxWidth()).isEqualTo(Duration.ofSeconds(20));
    }

    @Test
    void aWindowThatCannotGrowNeverReachesItsMaximum() {
        ExpandingWindow fixed = new ExpandingWindow(100, 0, 600);
        assertThat(fixed.widthAfter(Duration.ofHours(1))).isEqualTo(100);
        assertThat(fixed.timeToMaxWidth().getSeconds()).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void rejectsAMaximumBelowTheStartingWidth() {
        assertThatThrownBy(() -> new ExpandingWindow(600, 25, 100))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
