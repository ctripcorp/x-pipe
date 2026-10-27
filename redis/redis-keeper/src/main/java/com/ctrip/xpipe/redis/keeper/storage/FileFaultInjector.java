package com.ctrip.xpipe.redis.keeper.storage;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.utils.XpipeThreadFactory;

/**
 * Injects EIO and delay into file channel operations of a debug-mode keeper (keeper-tfs-m6 §17.3).
 * One instance per AsyncFileSystem, shared by the debug factory and every channel it opens.
 *
 * Thread safety: config lives in an immutable snapshot. Only the refresh thread replaces it, by
 * swapping the volatile reference, so operations read it without locking and never read config.
 */
public class FileFaultInjector implements AutoCloseable {

    public enum Category { OPEN, READ, WRITE, FORCE }

    static final String EIO_MESSAGE = "Input/output error (keeper fs debug injected)";
    static final String ALL_OPS = "*";
    static final long SLEEP_SLICE_MILLI = 100;
    static final long REFRESH_INTERVAL_MILLI = 1000;
    private static final String THREAD_NAME = "keeper-fs-fault-refresh";

    private static final Logger logger = LoggerFactory.getLogger(FileFaultInjector.class);

    private final DebugFileChannelConfig config;
    private final long refreshIntervalMilli;
    private volatile Snapshot snapshot;
    // Guarded by this; touched only by start / close.
    private ScheduledExecutorService refresher;
    private boolean closed;

    public FileFaultInjector(DebugFileChannelConfig config) {
        this(config, REFRESH_INTERVAL_MILLI);
    }

    FileFaultInjector(DebugFileChannelConfig config, long refreshIntervalMilli) {
        this.config = Objects.requireNonNull(config, "config");
        this.refreshIntervalMilli = refreshIntervalMilli;
        this.snapshot = load();
        logger.warn("[FileFaultInjector] debug file fault injection created, refreshIntervalMilli={}, {}",
                refreshIntervalMilli, snapshot);
    }

    /** Starts reloading config every refresh interval. Idempotent; no-op after close. */
    public synchronized void start() {
        if (closed || refresher != null) {
            return;
        }
        ScheduledThreadPoolExecutor executor =
                new ScheduledThreadPoolExecutor(1, XpipeThreadFactory.create(THREAD_NAME, true));
        executor.scheduleWithFixedDelay(this::refreshQuietly, refreshIntervalMilli, refreshIntervalMilli,
                TimeUnit.MILLISECONDS);
        refresher = executor;
    }

    /** Stops the refresh; the last snapshot stays in effect. Idempotent. */
    @Override
    public synchronized void close() {
        closed = true;
        if (refresher != null) {
            refresher.shutdownNow();
            refresher = null;
        }
    }

    /** Applies the configured delay, then throws EIO if configured; returns normally otherwise. */
    public void inject(Category category, String path) throws IOException {
        Snapshot current = snapshot;
        if (!current.injects(category, path)) {
            return;
        }
        current = sleepIfNeeded(current, category, path);
        if (current.eio && current.injects(category, path)) {
            throw new IOException(EIO_MESSAGE);
        }
    }

    // Sleeps in slices so a smaller delay, or injection turned off, releases a waiting operation
    // within one refresh interval plus one slice. Returns the snapshot seen last.
    private Snapshot sleepIfNeeded(Snapshot current, Category category, String path) {
        long startNanos = System.nanoTime();
        while (current.injects(category, path)) {
            long remaining = current.delayMilli - TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            if (remaining <= 0) {
                break;
            }
            try {
                Thread.sleep(Math.min(SLEEP_SLICE_MILLI, remaining));
            } catch (InterruptedException e) {
                // The real operation still runs and reacts to the interrupt per JDK semantics.
                Thread.currentThread().interrupt();
                break;
            }
            current = snapshot;
        }
        return current;
    }

    // Single writer: the refresh thread (tests call it directly instead of starting the thread).
    void refresh() {
        Snapshot fresh = load();
        Snapshot previous = snapshot;
        if (!fresh.sameConfig(previous)) {
            logger.warn("[FileFaultInjector] fault config changed, {} -> {}", previous, fresh);
        }
        snapshot = fresh;
    }

    private void refreshQuietly() {
        try {
            refresh();
        } catch (Throwable t) {
            logger.warn("[FileFaultInjector] refresh failed, keeping {}", snapshot, t);
        }
    }

    private Snapshot load() {
        try {
            return new Snapshot(config.isEio(), config.getDelayMilli(), parseOps(config.getOps()),
                    config.getPathContains(), null);
        } catch (RuntimeException e) {
            // An unreadable config injects nothing; the change log reports it once.
            return new Snapshot(false, 0, EnumSet.noneOf(Category.class), null,
                    e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    // "*" anywhere in the list selects every category; empty or null selects none.
    static Set<Category> parseOps(String raw) {
        Set<Category> categories = EnumSet.noneOf(Category.class);
        if (raw == null) {
            return categories;
        }
        for (String name : raw.split(",")) {
            String trimmed = name.trim().toUpperCase(Locale.ROOT);
            if (ALL_OPS.equals(trimmed)) {
                return EnumSet.allOf(Category.class);
            }
            for (Category category : Category.values()) {
                if (category.name().equals(trimmed)) {
                    categories.add(category);
                }
            }
        }
        return categories;
    }

    private static final class Snapshot {
        final boolean eio;
        final long delayMilli;
        final Set<Category> ops;
        final String pathContains;
        final String error;

        Snapshot(boolean eio, long delayMilli, Set<Category> ops, String pathContains, String error) {
            this.eio = eio;
            this.delayMilli = delayMilli;
            this.ops = ops;
            this.pathContains = pathContains;
            this.error = error;
        }

        boolean injects(Category category, String path) {
            if (!eio && delayMilli <= 0) {
                return false;
            }
            if (!ops.contains(category)) {
                return false;
            }
            return pathContains == null || pathContains.isEmpty()
                    || (path != null && path.contains(pathContains));
        }

        boolean sameConfig(Snapshot other) {
            return eio == other.eio && delayMilli == other.delayMilli && ops.equals(other.ops)
                    && Objects.equals(pathContains, other.pathContains) && Objects.equals(error, other.error);
        }

        @Override
        public String toString() {
            if (error != null) {
                return "config read failed (" + error + "), injection disabled";
            }
            return "eio=" + eio + ", delayMilli=" + delayMilli + ", ops=" + ops + ", pathContains="
                    + (pathContains == null ? "" : pathContains);
        }
    }
}
