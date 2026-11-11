package com.ctrip.xpipe.redis.keeper.storage.health;

import com.ctrip.xpipe.redis.keeper.storage.EIOException;
import com.ctrip.xpipe.redis.keeper.storage.StorageIOException;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.ClosedChannelException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;

/**
 * Counts every task submitted to the container IO executor, per container / tenant / file, over a
 * fixed 30s window. One instance per process.
 */
public final class IoTelemetry {

    private static final Logger logger = LoggerFactory.getLogger(IoTelemetry.class);

    public static final IoTelemetry INSTANCE = new IoTelemetry();

    /** The ring is one second per bucket, so a window wider than this cannot be served. */
    public static final long MAX_WINDOW_NANOS = TimeUnit.SECONDS.toNanos(30);

    static final int BUCKETS = 32;
    static final long BUCKET_NANOS = TimeUnit.SECONDS.toNanos(1);
    /** Latest failure timestamps kept per file; enough for any "N in the window" threshold. */
    static final int FILE_FAIL_RING = 16;

    private final AtomicReferenceArray<Bucket> containerWindows = new AtomicReferenceArray<>(BUCKETS);
    private final ConcurrentHashMap<String, TenantHealth> tenantHealth = new ConcurrentHashMap<>();
    /** Tasks currently inside {@code run()}: bounded by the pool size, never by the queue depth. */
    private final Set<IoTask<?>> executing = ConcurrentHashMap.newKeySet();
    private final AtomicLong recordedCount = new AtomicLong();
    private volatile long startedNanos = System.nanoTime();
    private volatile IoTelemetrySettings settings = IoTelemetrySettings.DEFAULTS;

    /** Tests drive the windows with a fake clock. */
    @VisibleForTesting
    public LongSupplier clock = System::nanoTime;

    @VisibleForTesting
    public IoTelemetry() {
    }

    /** Refreshed by the health checker each tick; defaults until then. */
    public void applySettings(IoTelemetrySettings settings) {
        if (settings != null) {
            this.settings = settings;
        }
    }

    /** Marks the task as actually running, ending its queue wait. */
    public void taskStarted(IoTask<?> task) {
        try {
            if (!settings.enabled()) {
                return;
            }
            task.startNanos = clock.getAsLong();
            executing.add(task);
        }catch (Throwable ignored){
        }
    }

    public void taskCompleted(IoTask<?> task, Throwable error) {
        try {
            if (!settings.enabled()) {
                return;
            }
            // before record(): a throwing recorder must not leave the entry behind for good
            executing.remove(task);
            long endNanos = clock.getAsLong();
            long startNanos = task.startNanos == 0 ? endNanos : task.startNanos;
            boolean failed = isFailure(error);
            record(endNanos, task.tenant, task.path, endNanos - startNanos, endNanos - task.submitNanos, failed);
            recordedCount.incrementAndGet();
            if (failed) {
                logger.debug("[taskCompleted][fail]{}, {}", task.tenant, task.path, error);
            }
        }catch (Throwable ignored){
        }
    }

    public long recordedCount() {
        return recordedCount.get();
    }

    public long startedNanos() {
        return startedNanos;
    }

    /** Number of tasks currently running; bounded by the pool size, never by the queue depth. */
    public int executingCount() {
        return executing.size();
    }

    public IoHealthSnapshot snapshot(long nowNanos) {
        IoTelemetrySettings settings = this.settings;
        // buckets before executing: a task finishing in between is missed for one tick at worst
        WindowAgg container = scanWindow(containerWindows, nowNanos, settings.windowNanos());
        Map<String, IoHealthSnapshot.TenantStat> tenants = tenantStats(nowNanos, settings);

        // exec time is measured from startNanos, so queue wait is not counted
        long inFlight = 0;
        long slowInFlight = 0;
        long maxInFlightNanos = 0;
        for (IoTask<?> task : executing) {
            long execNanos = nowNanos - task.startNanos;
            boolean slow = execNanos > settings.slowNanos();
            inFlight++;
            if (slow) {
                slowInFlight++;
            }
            maxInFlightNanos = Math.max(maxInFlightNanos, execNanos);
            if (task.tenant != null) {
                IoHealthSnapshot.TenantStat stat =
                        tenants.computeIfAbsent(task.tenant, t -> new IoHealthSnapshot.TenantStat());
                stat.inFlight++;
                if (slow) {
                    stat.slowInFlight++;
                }
            }
        }

        return new IoHealthSnapshot(nowNanos, container.total, container.failed, container.slow,
                inFlight, slowInFlight, maxInFlightNanos, container.maxEndToEndNanos,
                container.maxExecNanos, tenants);
    }

    private Map<String, IoHealthSnapshot.TenantStat> tenantStats(long nowNanos, IoTelemetrySettings settings) {
        Map<String, IoHealthSnapshot.TenantStat> stats = new HashMap<>();
        long idleBefore = nowNanos - settings.stateIdleNanos();
        long fileWindowStart = nowNanos - settings.fileWindowNanos();
        for (Map.Entry<String, TenantHealth> entry : tenantHealth.entrySet()) {
            TenantHealth health = entry.getValue();
            WindowAgg agg = scanWindow(health.buckets, nowNanos, settings.windowNanos());
            Map<String, IoHealthSnapshot.FileStat> files = fileStats(health.files, fileWindowStart, idleBefore);
            if (agg.total > 0 || !files.isEmpty()) {
                IoHealthSnapshot.TenantStat stat = new IoHealthSnapshot.TenantStat();
                stat.total = agg.total;
                stat.abnormal = agg.abnormal;
                stat.files.putAll(files);
                stats.put(entry.getKey(), stat);
            }
            // idle eviction uses the newest write, not the rule window; files go with the tenant
            long newestWrite = newestWriteNanos(health.buckets);
            if (newestWrite == Long.MIN_VALUE || newestWrite < idleBefore) {
                tenantHealth.remove(entry.getKey(), health);
            }
        }
        return stats;
    }

    private Map<String, IoHealthSnapshot.FileStat> fileStats(Map<String, FileState> files,
            long windowStart, long idleBefore) {
        Map<String, IoHealthSnapshot.FileStat> stats = new HashMap<>();
        for (Map.Entry<String, FileState> entry : files.entrySet()) {
            FileState state = entry.getValue();
            if (state.lastTouchedNanos() < idleBefore) {
                files.remove(entry.getKey(), state);
                continue;
            }
            IoHealthSnapshot.FileStat stat = state.snapshot(windowStart);
            if (stat != null) {
                stats.put(entry.getKey(), stat);
            }
        }
        return stats;
    }

    private static WindowAgg scanWindow(AtomicReferenceArray<Bucket> buckets, long nowNanos, long windowNanos) {
        WindowAgg agg = new WindowAgg();
        long windowStart = nowNanos - windowNanos;
        long newestId = Math.floorDiv(nowNanos, BUCKET_NANOS);
        long windowSeconds = windowNanos / BUCKET_NANOS;
        for (long offset = 0; offset < windowSeconds; offset++) {
            long id = newestId - offset;
            long expectedStart = id * BUCKET_NANOS;
            if (expectedStart <= windowStart) {
                break;
            }
            int index = Math.floorMod(id, BUCKETS);
            Bucket bucket = buckets.get(index);
            if (bucket != null && bucket.startNanos == expectedStart) {
                agg.total += bucket.total.sum();
                agg.failed += bucket.failed.sum();
                agg.slow += bucket.slow.sum();
                agg.abnormal += bucket.abnormal.sum();
                agg.maxExecNanos = Math.max(agg.maxExecNanos, bucket.maxExecNanos.get());
                agg.maxEndToEndNanos = Math.max(agg.maxEndToEndNanos, bucket.maxEndToEndNanos.get());
            }
        }
        return agg;
    }

    /** Newest write in the ring, used for the 1-minute idle eviction. */
    private static long newestWriteNanos(AtomicReferenceArray<Bucket> buckets) {
        long newest = Long.MIN_VALUE;
        for (int i = 0; i < BUCKETS; i++) {
            Bucket bucket = buckets.get(i);
            if (bucket != null) {
                newest = Math.max(newest, bucket.startNanos);
            }
        }
        return newest;
    }

    /** One pass over a bucket ring's rule-window totals. */
    private static final class WindowAgg {
        long total;
        long failed;
        long slow;
        long abnormal;
        long maxExecNanos;
        long maxEndToEndNanos;
    }

    private void record(long endNanos, String tenant, String path, long execNanos, long endToEndNanos,
            boolean failed) {
        IoTelemetrySettings settings = this.settings;
        boolean slow = execNanos > settings.slowNanos();
        add(bucketFor(containerWindows, endNanos), execNanos, endToEndNanos, failed, slow);
        if (tenant == null) {
            return;
        }
        TenantHealth health = tenantHealth.computeIfAbsent(tenant, key -> new TenantHealth());
        add(bucketFor(health.buckets, endNanos), execNanos, endToEndNanos, failed, slow);
        // the path already carries the keeper's store dir, so it identifies the file on its own
        health.files.computeIfAbsent(path, key -> new FileState(tenant, path))
                .record(endNanos, failed);
    }

    private static void add(Bucket bucket, long execNanos, long endToEndNanos, boolean failed,
            boolean slow) {
        if (bucket == null) {
            return;
        }
        bucket.total.increment();
        if (failed) {
            bucket.failed.increment();
        }
        if (slow) {
            bucket.slow.increment();
        }
        if (failed || slow) {
            bucket.abnormal.increment();
        }
        accumulateMax(bucket.maxExecNanos, execNanos);
        accumulateMax(bucket.maxEndToEndNanos, endToEndNanos);
    }

    /**
     * The bucket covering {@code nowNanos}, rotating the slot to a fresh bucket once the second it
     * held has passed. A writer that races with the rotation may land in the old bucket, which is
     * about to leave the window: at most a handful of records around a second boundary are lost,
     * which is well inside the one-second granularity of the window itself.
     */
    private static Bucket bucketFor(AtomicReferenceArray<Bucket> buckets, long nowNanos) {
        long id = Math.floorDiv(nowNanos, BUCKET_NANOS);
        long start = id * BUCKET_NANOS;
        int index = Math.floorMod(id, BUCKETS);
        for (int attempt = 0; attempt < 3; attempt++) {
            Bucket current = buckets.get(index);
            if (current != null && current.startNanos == start) {
                return current;
            }
            Bucket fresh = new Bucket(start);
            if (buckets.compareAndSet(index, current, fresh)) {
                return fresh;
            }
        }
        Bucket current = buckets.get(index);
        return current != null && current.startNanos == start ? current : null;
    }

    private static void accumulateMax(AtomicLong target, long value) {
        long current = target.get();
        while (value > current && !target.compareAndSet(current, value)) {
            current = target.get();
        }
    }

    /**
     * Only EIO and StorageIOException mean the storage itself is unusable. A StorageIOException
     * that merely wraps a ClosedChannelException is our own close (shutdown, roll) and does not
     * count. Everything else is counted in total only.
     */
    static boolean isFailure(Throwable error) {
        Throwable cause = unwrap(error);
        if (cause instanceof EIOException) {
            return true;
        }
        if (cause instanceof StorageIOException) {
            return !(cause.getCause() instanceof ClosedChannelException);
        }
        return false;
    }

    private static Throwable unwrap(Throwable error) {
        Throwable current = error;
        for (int depth = 0; current != null && depth < 8; depth++) {
            if (current instanceof EIOException || current instanceof StorageIOException) {
                return current;
            }
            if (current instanceof CompletionException || current instanceof ExecutionException) {
                current = current.getCause();
                continue;
            }
            return current;
        }
        return current;
    }

    /** Visible for tests: drop all history. */
    void reset() {
        for (int i = 0; i < BUCKETS; i++) {
            containerWindows.set(i, null);
        }
        tenantHealth.clear();
        executing.clear();
        recordedCount.set(0);
    }

    /** Visible for tests: pretend the process has been up for a while (skips the warm-up gate). */
    public void startAt(long startedNanos) {
        this.startedNanos = startedNanos;
    }

    static final class Bucket {

        final long startNanos;
        final LongAdder total = new LongAdder();
        final LongAdder failed = new LongAdder();
        final LongAdder slow = new LongAdder();
        /** Failed or slow: the tenant rule counts both once per task. */
        final LongAdder abnormal = new LongAdder();
        final AtomicLong maxExecNanos = new AtomicLong();
        final AtomicLong maxEndToEndNanos = new AtomicLong();

        Bucket(long startNanos) {
            this.startNanos = startNanos;
        }
    }

    /** Per keeper: its window ring plus the files it has touched recently. */
    static final class TenantHealth {

        final AtomicReferenceArray<Bucket> buckets = new AtomicReferenceArray<>(BUCKETS);
        final ConcurrentHashMap<String, FileState> files = new ConcurrentHashMap<>();
    }

    /** Trailing failure run plus the latest failure timestamps of one file. */
    static final class FileState {

        private final String tenant;
        private final String path;
        private final long[] failNanos = new long[FILE_FAIL_RING];
        private int failIndex;
        private long consecutiveFails;
        private volatile long lastTouched;

        FileState(String tenant, String path) {
            this.tenant = tenant;
            this.path = path;
            Arrays.fill(failNanos, Long.MIN_VALUE);
        }

        synchronized void record(long nanos, boolean failed) {
            lastTouched = nanos;
            if (!failed) {
                // only a success ends a run; the window does not, so a broken idle file stays visible
                consecutiveFails = 0;
                return;
            }
            consecutiveFails++;
            failNanos[failIndex] = nanos;
            failIndex = (failIndex + 1) % failNanos.length;
        }

        long lastTouchedNanos() {
            return lastTouched;
        }

        /** Null when the file has seen no IO inside the window, exactly like an absent record. */
        synchronized IoHealthSnapshot.FileStat snapshot(long windowStart) {
            if (lastTouched < windowStart) {
                return null;
            }
            int fails = 0;
            for (long nanos : failNanos) {
                if (nanos >= windowStart) {
                    fails++;
                }
            }
            IoHealthSnapshot.FileStat stat = new IoHealthSnapshot.FileStat();
            stat.tenant = tenant;
            stat.path = path;
            stat.lastEndNanos = lastTouched;
            stat.consecutiveFails = consecutiveFails;
            stat.fails10 = fails;
            return stat;
        }
    }
}
