package com.ctrip.xpipe.redis.keeper.storage.health;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/** One IO call submitted to the container executor; reports its own start and finish. */
public final class IoTask<T> implements Runnable {

    final String tenant;
    final String path;
    final long submitNanos;
    /** Set when a pool thread picks the task up; 0 while it is still queued. */
    volatile long startNanos;

    private final Supplier<T> task;
    private final CompletableFuture<T> future = new CompletableFuture<>();

    public IoTask(String tenant, String path, long submitNanos, Supplier<T> task) {
        this.tenant = tenant;
        this.path = path;
        this.submitNanos = submitNanos;
        this.task = task;
    }

    public long submitNanos() {
        return submitNanos;
    }

    public CompletableFuture<T> future() {
        return future;
    }

    // same outcome as CompletableFuture.supplyAsync: the value or the throwable goes into the
    // future, and the pool thread never sees the failure escape into its worker loop
    @Override
    public void run() {
        IoTelemetry.INSTANCE.taskStarted(this);
        Throwable error = null;
        try {
            future.complete(task.get());
        } catch (Throwable t) {
            error = t;
            future.completeExceptionally(t);
        } finally {
            IoTelemetry.INSTANCE.taskCompleted(this, error);
        }
    }
}
