package com.ctrip.xpipe.redis.keeper.storage;

import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Keeps retrying the close of handles whose close failed, until the TailCache entry, chunks,
 * channels and lease are released (keeper-tfs-m6 spec §2). One instance per AsyncFileSystem,
 * owned by the container, independent of keeper lifecycles.
 * <p>
 * Concurrency: {@link #lock} guards both indices; any thread may defer or query. Each handle is
 * scheduled on its own and all attempts run on one dedicated thread, so a handle is never closed
 * concurrently and its mutable state has a single writer. That thread runs blocking closes by
 * design (spec §9).
 */
public final class DeferredHandleCloser {

    private static final Logger logger = LoggerFactory.getLogger(DeferredHandleCloser.class);

    static final String THREAD_NAME = "keeper-deferred-closer";
    static final long INITIAL_BACKOFF_MILLIS = 1000L;
    static final long MAX_BACKOFF_MILLIS = 30_000L;
    // Only bounds the shift; any exponent above 5 is already capped by MAX_BACKOFF_MILLIS.
    private static final int MAX_BACKOFF_EXPONENT = 30;
    // TailCache close is synchronous; this only guards an FS that returns a pending future.
    static final long CLOSE_AWAIT_TIMEOUT_MILLIS = 30_000L;
    static final long SHUTDOWN_AWAIT_MILLIS = 30_000L;
    static final long DEFAULT_FLUSH_WINDOW_MILLIS = 300_000L;

    private static final Map<AsyncFileSystem, DeferredHandleCloser> REGISTRY =
            Collections.synchronizedMap(new IdentityHashMap<>());

    private final AsyncFileSystem fs;
    private final LongSupplier flushWindowMillis;
    private final LongSupplier clock;
    private final ScheduledExecutorService executor;

    private final Object lock = new Object();
    private final Map<AbstractStorageFile, DeferredHandle> byFile = new IdentityHashMap<>();
    private final Map<String, List<DeferredHandle>> byKey = new HashMap<>();
    private boolean shutdown;

    public DeferredHandleCloser(AsyncFileSystem fs, LongSupplier flushWindowMillis) {
        this(fs, flushWindowMillis, System::currentTimeMillis, newExecutor());
    }

    DeferredHandleCloser(AsyncFileSystem fs, LongSupplier flushWindowMillis, LongSupplier clock,
                         ScheduledExecutorService executor) {
        this.fs = fs;
        this.flushWindowMillis = flushWindowMillis;
        this.clock = clock;
        this.executor = executor;
    }

    private static ScheduledExecutorService newExecutor() {
        ScheduledThreadPoolExecutor executor =
                new ScheduledThreadPoolExecutor(1, XpipeThreadFactory.create(THREAD_NAME, true));
        // shutdown() drops queued retries and only waits for the attempt in progress.
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        return executor;
    }

    public static void register(AsyncFileSystem fs, DeferredHandleCloser closer) {
        if (fs == null || closer == null || closer.fs != fs) {
            throw new IllegalArgumentException("closer must be registered with the FS it closes on");
        }
        REGISTRY.put(fs, closer);
    }

    public static DeferredHandleCloser of(AsyncFileSystem fs) {
        return fs == null ? null : REGISTRY.get(fs);
    }

    /**
     * Takes ownership of {@code file}; the caller must not touch it afterwards. {@code replId} is
     * the keeper the handle belongs to, as known by the close caller. Deferring the same object
     * again is ignored. Returns false only after {@link #shutdown()}.
     */
    public boolean defer(AbstractStorageFile file, ReplId replId, String operation, Throwable cause) {
        long window = readFlushWindowMillis();
        DeferredHandle handle;
        synchronized (lock) {
            if (shutdown) {
                logger.error("[deferred-closer][rejected][shutdown] repl_id={} fileKey={} {}",
                        DeferredHandle.replIdText(replId), DeferredHandle.displayKey(file.getKey()), operation, cause);
                return false;
            }
            DeferredHandle existing = byFile.get(file);
            if (existing != null) {
                logger.warn("[deferred-closer][duplicate] {}", existing);
                return true;
            }
            handle = new DeferredHandle(fs, file, replId, operation, cause, clock.getAsLong(), window);
            byFile.put(file, handle);
            byKey.computeIfAbsent(file.getKey(), k -> new ArrayList<>(1)).add(handle);
        }
        long delay = backoffMillis(handle.getBackoffFailures());
        logger.warn("[deferred-closer][deferred] {} first retry in {}ms", handle, delay, cause);
        schedule(handle, delay);
        return true;
    }

    public boolean isPending(AbstractStorageFile file) {
        synchronized (lock) {
            return byFile.containsKey(file);
        }
    }

    /** A pending handle with this FS file key, preferring a write handle; null if none. */
    public DeferredHandle findPending(String key) {
        synchronized (lock) {
            List<DeferredHandle> handles = byKey.get(key);
            if (handles == null || handles.isEmpty()) {
                return null;
            }
            for (DeferredHandle handle : handles) {
                if (handle.isWrite()) {
                    return handle;
                }
            }
            return handles.get(0);
        }
    }

    public int count() {
        synchronized (lock) {
            return byFile.size();
        }
    }

    public int writeCount() {
        synchronized (lock) {
            int count = 0;
            for (DeferredHandle handle : byFile.values()) {
                if (handle.isWrite()) {
                    count++;
                }
            }
            return count;
        }
    }

    /** Pending handles, oldest first. Each field is a recent read; the list itself is a copy. */
    public List<DeferredHandle> snapshot() {
        List<DeferredHandle> handles;
        synchronized (lock) {
            handles = new ArrayList<>(byFile.values());
        }
        handles.sort(Comparator.comparingLong(DeferredHandle::getFirstFailureTimeMillis));
        return Collections.unmodifiableList(handles);
    }

    public List<DeferredHandle> shutdown() {
        return shutdown(SHUTDOWN_AWAIT_MILLIS);
    }

    /**
     * Stops accepting handles, drops queued retries and waits up to {@code awaitMillis} for the
     * attempt in progress. Never interrupts: an interrupted close may leave the handle half closed.
     * Returns and logs the handles left unclosed.
     */
    List<DeferredHandle> shutdown(long awaitMillis) {
        synchronized (lock) {
            if (shutdown) {
                return snapshot();
            }
            shutdown = true;
        }
        REGISTRY.remove(fs, this);
        executor.shutdown();
        boolean terminated;
        try {
            terminated = executor.awaitTermination(awaitMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            terminated = false;
        }
        if (!terminated) {
            logger.error("[deferred-closer][shutdown] close in progress not finished within {}ms", awaitMillis);
        }
        List<DeferredHandle> leftover = snapshot();
        if (!leftover.isEmpty()) {
            logger.error("[deferred-closer][shutdown] {} handles left unclosed: {}", leftover.size(), leftover);
        }
        return leftover;
    }

    static long backoffMillis(int failures) {
        int exponent = Math.max(0, Math.min(failures - 1, MAX_BACKOFF_EXPONENT));
        return Math.min(INITIAL_BACKOFF_MILLIS << exponent, MAX_BACKOFF_MILLIS);
    }

    private long readFlushWindowMillis() {
        try {
            return flushWindowMillis.getAsLong();
        } catch (Throwable t) {
            logger.warn("[deferred-closer] failed to read flush window, use default {}ms",
                    DEFAULT_FLUSH_WINDOW_MILLIS, t);
            return DEFAULT_FLUSH_WINDOW_MILLIS;
        }
    }

    private void schedule(DeferredHandle handle, long delayMillis) {
        handle.setNextRetryTimeMillis(clock.getAsLong() + delayMillis);
        try {
            executor.schedule(() -> runAttempt(handle), delayMillis, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            // Only after shutdown: the handle stays registered and is reported as left over.
            logger.warn("[deferred-closer][schedule-rejected] {}", handle, e);
        }
    }

    void runAttempt(DeferredHandle handle) {
        try {
            attempt(handle);
        } catch (Throwable t) {
            // attempt() handles close failures itself; this keeps a bookkeeping bug from dropping
            // the handle.
            logger.error("[deferred-closer][unexpected] {}", handle, t);
            schedule(handle, MAX_BACKOFF_MILLIS);
        }
    }

    private void attempt(DeferredHandle handle) {
        long now = clock.getAsLong();
        if (handle.currentMode() == DeferredHandle.CloseMode.FLUSH && now >= handle.getFlushDeadlineMillis()) {
            handle.markDegraded();
            logger.error("[deferred-closer][degrade-no-flush] flush window passed, unflushed data will be dropped: {}",
                    handle);
        }
        boolean noFlush = handle.currentMode() == DeferredHandle.CloseMode.NO_FLUSH;
        Throwable failure;
        try {
            awaitClose(invokeClose(handle.file, noFlush));
            dequeue(handle);
            logger.info("[deferred-closer][closed] {} age={}ms", handle, now - handle.getFirstFailureTimeMillis());
            return;
        } catch (IllegalStateException | IllegalArgumentException e) {
            // Programming error such as "close is not allowed"; retrying cannot succeed.
            dequeue(handle);
            logger.error("[deferred-closer][drop] {}", handle, e);
            return;
        } catch (Throwable t) {
            failure = t;
        }
        long delay = backoffMillis(handle.recordFailure(failure));
        logger.warn("[deferred-closer][retry-fail] {} next retry in {}ms", handle, delay, failure);
        schedule(handle, delay);
    }

    private CompletableFuture<Void> invokeClose(AbstractStorageFile file, boolean noFlush) {
        if (file instanceof AsyncFile) {
            return fs.close((AsyncFile) file, noFlush);
        }
        if (file instanceof AsyncSegmentFile) {
            return fs.close((AsyncSegmentFile) file, noFlush);
        }
        throw new IllegalArgumentException("unsupported storage file type: " + file.getClass().getName());
    }

    private static void awaitClose(CompletableFuture<Void> future) throws Throwable {
        if (future == null) {
            return;
        }
        try {
            future.get(CLOSE_AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw e.getCause() != null ? e.getCause() : e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        }
    }

    private void dequeue(DeferredHandle handle) {
        synchronized (lock) {
            if (byFile.get(handle.file) == handle) {
                byFile.remove(handle.file);
            }
            List<DeferredHandle> handles = byKey.get(handle.getFileKey());
            if (handles != null) {
                handles.removeIf(h -> h == handle);
                if (handles.isEmpty()) {
                    byKey.remove(handle.getFileKey());
                }
            }
        }
    }
}
