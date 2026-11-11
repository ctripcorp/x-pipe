package com.ctrip.xpipe.redis.keeper.storage.health;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/** One IO call submitted to the container executor; reports its own start and finish. */
public final class IoTask<T> implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(IoTask.class);


    final String tenant;
    final String path;
    final long submitNanos;
    final IoSource source;
    /** Set when a pool thread picks the task up; 0 while it is still queued. */
    volatile long startNanos;

    public static final class Result<T> {
        private final T value;
        private final List<Exception> sideFailures;

        private Result(T value, List<Exception> sideFailures) {
            this.value = value;
            this.sideFailures = sideFailures;
        }

        public static <T> Result<T> success(T value) {
            return new Result<>(value, null);
        }

        public static <T> Result<T> withSideFailures(T value, List<Exception> sideFailures) {
            return new Result<>(value, sideFailures);
        }

        public T value() {
            return value;
        }

        public List<Exception> sideFailures() {
            return sideFailures;
        }
    }

    private final Supplier<Result<T>> task;
    private final CompletableFuture<T> future = new CompletableFuture<>();

    public IoTask(String tenant, String path, long submitNanos, Supplier<Result<T>> task) {
        this(tenant, path, submitNanos, IoSource.BUSINESS, task);
    }

    public IoTask(String tenant, String path, long submitNanos, IoSource source, Supplier<Result<T>> task) {
        this.tenant = tenant;
        this.path = path;
        this.submitNanos = submitNanos;
        this.source = source == null ? IoSource.BUSINESS : source;
        this.task = task;
    }

    public IoSource source() {
        return source;
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
            Result<T> result = task.get();
            if (result.sideFailures() == null) {
                future.complete(result.value());
            } else if(!result.sideFailures().isEmpty()) {
                error = result.sideFailures().getFirst();
                future.completeExceptionally(error);
            }
        } catch (Throwable t) {
            error = t;
            future.completeExceptionally(t);
        } finally {
            IoTelemetry.INSTANCE.taskCompleted(this, error);
        }
    }
}
