package com.cloudrelay.analytics;

import com.cloudrelay.event.SessionEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tees session events onto Kafka for the lakehouse.
 *
 * <h2>The rule this class exists to obey</h2>
 *
 * <p>A broker outage must never fail, slow, or block a game session. Analytics
 * is downstream of the product in every sense, and the failure mode where a
 * Kafka problem becomes a player-visible problem is the one thing worth
 * engineering against here.
 *
 * <p>Three things enforce that, and each one closes a gap the others leave open.
 *
 * <p><b>The send is handed to a bounded executor.</b> {@code KafkaProducer.send}
 * looks asynchronous and mostly is, but it blocks the calling thread for up to
 * {@code max.block.ms} when cluster metadata is unavailable or the accumulator
 * is full — which is exactly the situation during an outage. Off the request
 * thread, that block costs a background thread instead of a player's response.
 *
 * <p><b>The queue is bounded and overflow is discarded.</b> An unbounded queue
 * in front of a broker that is down is just a slower way to run out of heap. A
 * bounded queue with a discard policy converts a broker outage into a
 * measurable gap in analytics data, which is the correct thing to lose.
 *
 * <p><b>The drop is counted, not silent.</b> {@code cloudrelay_analytics_events_dropped_total}
 * going up is the signal that the pipeline is missing data. Silently dropping
 * would leave the lakehouse quietly incomplete with nothing to alert on, which
 * is worse than the outage.
 */
@Slf4j
public final class KafkaAnalyticsSink implements AnalyticsEventSink {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String topic;
    private final ThreadPoolExecutor executor;

    private final Counter published;
    private final Counter failed;
    private final Counter dropped;
    private final AtomicLong lastFailureLoggedAt = new AtomicLong();

    public KafkaAnalyticsSink(KafkaTemplate<String, String> kafkaTemplate,
                              ObjectMapper objectMapper,
                              String topic,
                              int queueCapacity,
                              int threads,
                              MeterRegistry meterRegistry) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.topic = topic;
        this.executor = new ThreadPoolExecutor(
                threads, threads, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(queueCapacity),
                runnable -> {
                    Thread t = new Thread(runnable, "analytics-tee");
                    t.setDaemon(true);
                    return t;
                },
                // Discard on overflow, but count it. ThreadPoolExecutor's own
                // DiscardPolicy drops silently, and a silent drop is the one
                // outcome this class must not have.
                (runnable, pool) -> countDrop());

        this.published = Counter.builder("cloudrelay.analytics.events.published")
                .description("Session events accepted by the Kafka broker")
                .register(meterRegistry);
        this.failed = Counter.builder("cloudrelay.analytics.events.failed")
                .description("Session events the broker rejected or timed out")
                .register(meterRegistry);
        this.dropped = Counter.builder("cloudrelay.analytics.events.dropped")
                .description("Session events discarded because the tee queue was full")
                .register(meterRegistry);
        meterRegistry.gauge("cloudrelay.analytics.queue.depth", executor,
                e -> e.getQueue().size());
    }

    @Override
    public void accept(SessionEvent event) {
        executor.execute(() -> send(event));
    }

    /**
     * Called by the rejection handler when the queue is full.
     *
     * <p>The counter is built in the constructor and the handler is installed
     * before it, so this reads the field defensively: a rejection cannot
     * actually happen that early, but a null check is cheaper than reasoning
     * about it.
     */
    private void countDrop() {
        Counter counter = this.dropped;
        if (counter != null) {
            counter.increment();
        }
    }

    private void send(SessionEvent event) {
        try {
            String payload = objectMapper.writeValueAsString(event);
            // Keyed by session code so every event for one session lands on one
            // partition and stays in order. The lakehouse does not depend on
            // that ordering, but it makes the raw topic readable when somebody
            // is reading offsets by hand at three in the morning.
            kafkaTemplate.send(topic, event.getSessionCode(), payload)
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
                            published.increment();
                        } else {
                            recordFailure(ex);
                        }
                    });
        } catch (Exception ex) {
            // Serialization failure, or send() throwing before it ever returns
            // a future. Either way the session operation has already succeeded
            // and is none of this method's business.
            recordFailure(ex);
        }
    }

    private void recordFailure(Throwable ex) {
        failed.increment();
        // A broker outage produces one of these per event. Log at most one a
        // minute so an outage does not turn into a second incident in the log
        // pipeline.
        long now = System.currentTimeMillis();
        long last = lastFailureLoggedAt.get();
        if (now - last > 60_000 && lastFailureLoggedAt.compareAndSet(last, now)) {
            log.warn("Analytics tee failing; session events are not reaching Kafka", ex);
        }
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    /** Visible for tests: how many events were discarded by backpressure. */
    public double droppedCount() {
        return dropped.count();
    }

    /** Drains in-flight sends on shutdown so a clean stop does not lose the tail. */
    public void shutdown() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
    }
}
