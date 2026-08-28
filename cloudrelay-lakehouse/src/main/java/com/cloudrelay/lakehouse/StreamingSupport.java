package com.cloudrelay.lakehouse;

import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryProgress;
import org.apache.spark.sql.streaming.Trigger;

/** Shared wiring for the streaming queries: trigger choice and checkpointing. */
public final class StreamingSupport {

    private StreamingSupport() {
    }

    /**
     * Applies the configured trigger and the checkpoint location.
     *
     * <p>The checkpoint is the whole exactly-once story. It holds the source
     * offsets that have been committed and the state store for the dedup
     * operator, and Delta commits the data files in the same logical step. A
     * job that dies between the two restarts from the last committed offset and
     * rewrites the batch; Delta's atomic commit means the half written attempt
     * was never visible, and the dedup operator drops anything the rewrite
     * duplicates. Losing the checkpoint directory is therefore not a cache
     * miss, it is data loss, which is why it lives inside the lakehouse and not
     * in a temp directory.
     */
    /**
     * Runs a {@code RATE_LIMITED} query until its source is exhausted, then
     * stops it.
     *
     * <p>A continuous trigger never ends on its own, so "caught up" has to be
     * observed rather than awaited: two consecutive batches that read nothing
     * mean the backlog is gone. Two rather than one because the first empty
     * batch can simply be a batch that started before the previous one's files
     * were visible.
     */
    public static void awaitCaughtUp(StreamingQuery query)
            throws InterruptedException, java.util.concurrent.TimeoutException {
        int consecutiveEmptyBatches = 0;
        long lastSeenBatchId = -1;

        while (query.isActive() && consecutiveEmptyBatches < 2) {
            StreamingQueryProgress[] progress = query.recentProgress();
            if (progress.length > 0) {
                StreamingQueryProgress latest = progress[progress.length - 1];
                if (latest.batchId() != lastSeenBatchId) {
                    lastSeenBatchId = latest.batchId();
                    consecutiveEmptyBatches =
                            latest.numInputRows() == 0 ? consecutiveEmptyBatches + 1 : 0;
                }
            }
            Thread.sleep(50);
        }
        query.stop();
    }

    public static <T> DataStreamWriter<T> configure(
            DataStreamWriter<T> writer, LakehouseOptions options, String queryName) {
        DataStreamWriter<T> configured = writer
                .queryName(queryName)
                .option("checkpointLocation", options.checkpointPath(queryName));
        return switch (options.triggerMode()) {
            case AVAILABLE_NOW -> configured.trigger(Trigger.AvailableNow());
            // Zero means "start the next batch as soon as the last one ends",
            // which is a normal streaming trigger and therefore does respect the
            // per-batch file limit. AvailableNow does not.
            case RATE_LIMITED -> configured.trigger(Trigger.ProcessingTime(0));
            case CONTINUOUS -> configured.trigger(
                    Trigger.ProcessingTime(options.processingInterval().toMillis()));
        };
    }
}
