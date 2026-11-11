package com.ctrip.xpipe.redis.keeper.storage.health;

/**
 * Submission-side IO classification. The source is captured by StorageUtil when it creates an
 * IoTask; the task does not depend on a ThreadLocal while it executes on the IO pool.
 */
public final class IoTelemetryContext {

    private static final ThreadLocal<IoSource> CURRENT =
            ThreadLocal.withInitial(() -> IoSource.BUSINESS);

    private IoTelemetryContext() {
    }

    public static IoSource currentSource() {
        return CURRENT.get();
    }

    public static Scope open(IoSource source) {
        IoSource previous = CURRENT.get();
        CURRENT.set(source == null ? IoSource.BUSINESS : source);
        return () -> CURRENT.set(previous);
    }

    public static Scope probe() {
        return open(IoSource.PROBE);
    }

    @FunctionalInterface
    public interface Scope extends AutoCloseable {
        @Override
        void close();
    }
}
