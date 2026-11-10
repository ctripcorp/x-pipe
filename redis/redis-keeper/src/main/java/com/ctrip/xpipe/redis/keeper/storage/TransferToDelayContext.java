package com.ctrip.xpipe.redis.keeper.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Best-effort timing context for one {@code ReferenceFileRegion.transferTo} invocation.
 * Monitoring failures are isolated from the replication path.
 */
public interface TransferToDelayContext {

    TransferToDelayContext NONE = new TransferToDelayContext() {
        @Override
        public boolean isEnabled() {
            return false;
        }

        @Override
        public void begin(Stage stage) {
        }

        @Override
        public void end(Stage stage) {
        }

        @Override
        public void complete(long transferredBytes, Throwable failure) {
        }
    };

    enum Stage {
        CACHE_GET,
        CACHE_TRANSFER,
        INFLIGHT_WAIT,
        IO_QUEUE,
        FILE_TRANSFER
    }

    boolean isEnabled();

    void begin(Stage stage);

    void end(Stage stage);

    void complete(long transferredBytes, Throwable failure);

    static void safeBegin(TransferToDelayContext context, Stage stage) {
        if (context == null) {
            return;
        }
        try {
            if (context.isEnabled()) {
                context.begin(stage);
            }
        } catch (Throwable t) {
            TransferToDelayMonitorSupport.logFailure("begin " + stage, t);
        }
    }

    static void safeEnd(TransferToDelayContext context, Stage stage) {
        if (context == null) {
            return;
        }
        try {
            if (context.isEnabled()) {
                context.end(stage);
            }
        } catch (Throwable t) {
            TransferToDelayMonitorSupport.logFailure("end " + stage, t);
        }
    }

    static void safeComplete(TransferToDelayContext context, long transferredBytes, Throwable failure) {
        if (context == null) {
            return;
        }
        try {
            if (context.isEnabled()) {
                context.complete(transferredBytes, failure);
            }
        } catch (Throwable t) {
            TransferToDelayMonitorSupport.logFailure("complete", t);
        }
    }
}

final class TransferToDelayMonitorSupport {

    private static final Logger logger = LoggerFactory.getLogger(TransferToDelayContext.class);

    private static final long RESET_QUIET_NANOS = TimeUnit.SECONDS.toNanos(30);

    private static final AtomicLong MONITOR_FAILURES = new AtomicLong();

    private static final AtomicLong LAST_FAILURE_NANOS = new AtomicLong();

    private TransferToDelayMonitorSupport() {
    }

    static void logFailure(String operation, Throwable throwable) {
        long now = System.nanoTime();
        long lastFailure = LAST_FAILURE_NANOS.getAndSet(now);
        if (lastFailure != 0L && now - lastFailure >= RESET_QUIET_NANOS) {
            long resetCount = MONITOR_FAILURES.getAndSet(0L);
            logger.info("[transferToDelay][monitor fail][reset][count={}]", resetCount);
        }

        long failures = MONITOR_FAILURES.incrementAndGet();
        if (isPowerOfTwo(failures)) {
            logger.warn("[transferToDelay][monitor fail][count={}] {}", failures, operation, throwable);
        }
    }

    private static boolean isPowerOfTwo(long value) {
        return value > 0 && (value & (value - 1)) == 0;
    }
}
