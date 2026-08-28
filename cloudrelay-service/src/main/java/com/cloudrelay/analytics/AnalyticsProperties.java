package com.cloudrelay.analytics;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration for the analytics tee, under {@code cloudrelay.analytics}.
 *
 * <p>Off by default. A developer running the service, and every unit test,
 * should get a working system without a broker anywhere near it.
 */
@ConfigurationProperties(prefix = "cloudrelay.analytics")
public class AnalyticsProperties {

    /** Whether session events are teed to Kafka at all. */
    private boolean enabled = false;

    private String bootstrapServers = "localhost:9092";

    private String topic = "cloudrelay.session-events";

    /**
     * How many events may be waiting to be sent before new ones are discarded.
     *
     * <p>At 700 req/s and a broker that has just gone away, this is roughly
     * three seconds of grace before the tee starts shedding. Larger would only
     * mean holding more events that are equally not going anywhere.
     */
    private int queueCapacity = 2000;

    private int senderThreads = 2;

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }

    public String getBootstrapServers() { return bootstrapServers; }
    public void setBootstrapServers(String v) { this.bootstrapServers = v; }

    public String getTopic() { return topic; }
    public void setTopic(String topic) { this.topic = topic; }

    public int getQueueCapacity() { return queueCapacity; }
    public void setQueueCapacity(int queueCapacity) { this.queueCapacity = queueCapacity; }

    public int getSenderThreads() { return senderThreads; }
    public void setSenderThreads(int senderThreads) { this.senderThreads = senderThreads; }
}
