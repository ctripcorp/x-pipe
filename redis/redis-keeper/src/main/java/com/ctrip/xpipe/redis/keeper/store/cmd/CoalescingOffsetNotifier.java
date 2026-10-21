package com.ctrip.xpipe.redis.keeper.store.cmd;

import com.ctrip.xpipe.utils.OffsetNotifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.IntSupplier;

/**
 * Coalesces command offset notifications by bytes / time threshold. All state is guarded by this monitor.
 */
public class CoalescingOffsetNotifier {

    private static final Logger logger = LoggerFactory.getLogger(CoalescingOffsetNotifier.class);

    private static final int MIN_BYTES_THRESHOLD = 1;

    private static final int MIN_TIME_MILLI_THRESHOLD = 1;

    private final OffsetNotifier offsetNotifier;

    private final IntSupplier bytesThreshold;

    private final IntSupplier timeMilliThreshold;

    private final BooleanSupplier coalescingEnabled;

    private final ScheduledExecutorService scheduler;

    private long pendingBytes;

    private long latestOffset = Long.MIN_VALUE;

    private boolean closed;

    private ScheduledFuture<?> scheduledFuture;

    /** @param scheduler null disables coalescing */
    public CoalescingOffsetNotifier(OffsetNotifier offsetNotifier, IntSupplier bytesThreshold,
                                    IntSupplier timeMilliThreshold, BooleanSupplier coalescingEnabled,
                                    ScheduledExecutorService scheduler) {
        this.offsetNotifier = Objects.requireNonNull(offsetNotifier, "offsetNotifier");
        this.bytesThreshold = Objects.requireNonNull(bytesThreshold, "bytesThreshold");
        this.timeMilliThreshold = Objects.requireNonNull(timeMilliThreshold, "timeMilliThreshold");
        this.coalescingEnabled = Objects.requireNonNull(coalescingEnabled, "coalescingEnabled");
        this.scheduler = scheduler;
    }

    public synchronized void onWritten(int wroteBytes, long endOffset) {
        if (closed || wroteBytes <= 0) {
            return;
        }

        latestOffset = Math.max(latestOffset, endOffset);
        pendingBytes += wroteBytes;

        if (scheduler == null || !coalescingEnabled.getAsBoolean()) {
            notifyPending();
            cancelScheduled();
            return;
        }

        if (pendingBytes >= Math.max(MIN_BYTES_THRESHOLD, bytesThreshold.getAsInt())) {
            notifyPending();
            cancelScheduled();
            return;
        }

        if (scheduledFuture == null) {
            schedule();
        }
    }

    public synchronized void flush() {
        if (closed) {
            return;
        }
        notifyPending();
        cancelScheduled();
    }

    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        notifyPending();
        cancelScheduled();
    }

    private void schedule() {
        long delayMilli = Math.max(MIN_TIME_MILLI_THRESHOLD, timeMilliThreshold.getAsInt());
        try {
            scheduledFuture = scheduler.schedule(this::notifyFromTimer, delayMilli, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            logger.warn("[schedule][rejected, notify now] offset:{}, pendingBytes:{}, {}",
                    latestOffset, pendingBytes, e.toString());
            notifyPending();
        }
    }

    private synchronized void notifyFromTimer() {
        try {
            scheduledFuture = null;
            if (closed) {
                return;
            }
            notifyPending();
        } catch (Throwable th) {
            logger.warn("[notifyFromTimer][fail] offset:{}", latestOffset, th);
        }
    }

    private void notifyPending() {
        if (pendingBytes <= 0 || latestOffset == Long.MIN_VALUE) {
            return;
        }

        long offset = latestOffset;
        pendingBytes = 0;
        try {
            offsetNotifier.offsetIncreased(offset);
        } catch (Throwable th) {
            logger.warn("[notifyPending][offset notify fail] offset:{}", offset, th);
        }
    }

    private void cancelScheduled() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
            scheduledFuture = null;
        }
    }
}
