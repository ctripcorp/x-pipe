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

/** Counts instrumented business and probe IO separately over a fixed sliding window. */
public final class IoTelemetry {

    private static final Logger logger = LoggerFactory.getLogger(IoTelemetry.class);

    public static final IoTelemetry INSTANCE = new IoTelemetry();
    public static final long MAX_WINDOW_NANOS = TimeUnit.SECONDS.toNanos(30);
    static final int BUCKETS = 32;
    static final long BUCKET_NANOS = TimeUnit.SECONDS.toNanos(1);
    static final int FILE_FAIL_RING = 16;

    private final AtomicReferenceArray<Bucket> businessWindows = new AtomicReferenceArray<>(BUCKETS);
    private final AtomicReferenceArray<Bucket> probeWindows = new AtomicReferenceArray<>(BUCKETS);
    private final ConcurrentHashMap<String, TenantHealth> tenantHealth = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, FileState> probeFiles = new ConcurrentHashMap<>();
    private final Set<IoTask<?>> executing = ConcurrentHashMap.newKeySet();
    private final AtomicLong recordedCount = new AtomicLong();
    private volatile long startedNanos = System.nanoTime();
    private volatile IoTelemetrySettings settings = IoTelemetrySettings.DEFAULTS;

    @VisibleForTesting
    public LongSupplier clock = System::nanoTime;

    @VisibleForTesting
    public IoTelemetry() {
    }

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

    /** Records one logical probe round. Internal probe tasks are intentionally not used for R5. */
    public void recordProbeRound(String path, long durationNanos, boolean success) {
        if (!settings.enabled()) {
            return;
        }
        long now = clock.getAsLong();
        Bucket bucket = bucketFor(probeWindows, now);
        if (bucket != null) {
            bucket.probeRounds.increment();
            if (durationNanos > settings.slowNanos()) {
                bucket.probeSlowRounds.increment();
            }
        }
        probeFiles.computeIfAbsent(path, p -> new FileState(null, p))
                .record(now, success);
    }

    public long recordedCount() {
        return recordedCount.get();
    }

    public long startedNanos() {
        return startedNanos;
    }

    public int executingCount() {
        return executing.size();
    }

    public IoHealthSnapshot snapshot(long nowNanos) {
        IoTelemetrySettings current = settings;
        WindowAgg business = scanWindow(businessWindows, nowNanos, current.windowNanos());
        WindowAgg probe = scanWindow(probeWindows, nowNanos, current.probeWindowNanos());
        Map<String, IoHealthSnapshot.TenantStat> tenants = tenantStats(nowNanos, current);
        Map<String, IoHealthSnapshot.FileStat> probeFileStats = probeFileStats(nowNanos, current);

        long businessInFlight = 0;
        long businessSlowInFlight = 0;
        long businessMaxInFlight = 0;
        long probeInFlight = 0;
        long probeSlowInFlight = 0;
        long probeMaxInFlight = 0;

        for (IoTask<?> task : executing) {
            long execNanos = nowNanos - task.startNanos;
            boolean slow = execNanos > current.slowNanos();
            if (task.source() == IoSource.PROBE) {
                probeInFlight++;
                if (slow) {
                    probeSlowInFlight++;
                }
                probeMaxInFlight = Math.max(probeMaxInFlight, execNanos);
                continue;
            }

            businessInFlight++;
            if (slow) {
                businessSlowInFlight++;
            }
            businessMaxInFlight = Math.max(businessMaxInFlight, execNanos);
            if (task.tenant != null) {
                IoHealthSnapshot.TenantStat stat =
                        tenants.computeIfAbsent(task.tenant, t -> IoHealthSnapshot.TenantStat.of(0, 0, 0, 0));
                stat.inFlight++;
                if (slow) {
                    stat.slowInFlight++;
                }
            }
        }

        IoHealthSnapshot.SourceSnapshot businessSnapshot = new IoHealthSnapshot.SourceSnapshot(
                nowNanos, business.total, business.failed, business.slow,
                businessInFlight, businessSlowInFlight, businessMaxInFlight,
                business.maxEndToEndNanos, business.maxExecNanos);
        IoHealthSnapshot.SourceSnapshot probeTaskSnapshot = new IoHealthSnapshot.SourceSnapshot(
                nowNanos, probe.total, probe.failed, probe.slow,
                probeInFlight, probeSlowInFlight, probeMaxInFlight,
                probe.maxEndToEndNanos, probe.maxExecNanos);
        IoHealthSnapshot.ProbeSnapshot probeSnapshot = new IoHealthSnapshot.ProbeSnapshot(
                probeTaskSnapshot, probe.rounds, probe.slowRounds, probeFileStats);
        return new IoHealthSnapshot(nowNanos, businessSnapshot, probeSnapshot, tenants);
    }

    private Map<String, IoHealthSnapshot.TenantStat> tenantStats(
            long nowNanos, IoTelemetrySettings current) {
        Map<String, IoHealthSnapshot.TenantStat> stats = new HashMap<>();
        long idleBefore = nowNanos - current.stateIdleNanos();
        long fileWindowStart = nowNanos - current.fileWindowNanos();
        for (Map.Entry<String, TenantHealth> entry : tenantHealth.entrySet()) {
            TenantHealth health = entry.getValue();
            WindowAgg agg = scanWindow(health.buckets, nowNanos, current.windowNanos());
            Map<String, IoHealthSnapshot.FileStat> files =
                    fileStats(health.files, fileWindowStart, idleBefore);
            if (agg.total > 0 || !files.isEmpty()) {
                IoHealthSnapshot.TenantStat stat = IoHealthSnapshot.TenantStat.of(
                        agg.total, agg.abnormal, 0, 0);
                stat.files.putAll(files);
                stats.put(entry.getKey(), stat);
            }
            long newestWrite = newestWriteNanos(health.buckets);
            if (newestWrite == Long.MIN_VALUE || newestWrite < idleBefore) {
                tenantHealth.remove(entry.getKey(), health);
            }
        }
        return stats;
    }

    private Map<String, IoHealthSnapshot.FileStat> probeFileStats(
            long nowNanos, IoTelemetrySettings current) {
        Map<String, IoHealthSnapshot.FileStat> stats = new HashMap<>();
        long windowStart = nowNanos - current.fileWindowNanos();
        long idleBefore = nowNanos - current.stateIdleNanos();
        for (Map.Entry<String, FileState> entry : probeFiles.entrySet()) {
            FileState state = entry.getValue();
            if (state.lastTouchedNanos() < idleBefore) {
                probeFiles.remove(entry.getKey(), state);
                continue;
            }
            IoHealthSnapshot.FileStat stat = state.snapshot(windowStart);
            if (stat != null) {
                stats.put(entry.getKey(), stat);
            }
        }
        return stats;
    }

    private Map<String, IoHealthSnapshot.FileStat> fileStats(
            Map<String, FileState> files, long windowStart, long idleBefore) {
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

    private static WindowAgg scanWindow(
            AtomicReferenceArray<Bucket> buckets, long nowNanos, long windowNanos) {
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
                agg.rounds += bucket.probeRounds.sum();
                agg.slowRounds += bucket.probeSlowRounds.sum();
                agg.maxExecNanos = Math.max(agg.maxExecNanos, bucket.maxExecNanos.get());
                agg.maxEndToEndNanos = Math.max(agg.maxEndToEndNanos, bucket.maxEndToEndNanos.get());
            }
        }
        return agg;
    }

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

    private static final class WindowAgg {
        long total;
        long failed;
        long slow;
        long abnormal;
        long rounds;
        long slowRounds;
        long maxExecNanos;
        long maxEndToEndNanos;
    }

    private void record(long endNanos, IoSource source, String tenant, String path,
            long execNanos, long endToEndNanos, boolean failed) {
        IoTelemetrySettings current = settings;
        boolean slow = execNanos > current.slowNanos();
        AtomicReferenceArray<Bucket> windows =
                source == IoSource.PROBE ? probeWindows : businessWindows;
        add(bucketFor(windows, endNanos), execNanos, endToEndNanos, failed, slow);
        if (source != IoSource.BUSINESS || tenant == null) {
            return;
        }
        TenantHealth health = tenantHealth.computeIfAbsent(tenant, key -> new TenantHealth());
        add(bucketFor(health.buckets, endNanos), execNanos, endToEndNanos, failed, slow);
        health.files.computeIfAbsent(path, key -> new FileState(tenant, path))
                .record(endNanos, failed);
    }

    private static void add(Bucket bucket, long execNanos, long endToEndNanos,
            boolean failed, boolean slow) {
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

    @VisibleForTesting
    void reset() {
        for (int i = 0; i < BUCKETS; i++) {
            businessWindows.set(i, null);
            probeWindows.set(i, null);
        }
        tenantHealth.clear();
        probeFiles.clear();
        executing.clear();
        recordedCount.set(0);
    }

    @VisibleForTesting
    public void startAt(long startedNanos) {
        this.startedNanos = startedNanos;
    }

    static final class Bucket {
        final long startNanos;
        final LongAdder total = new LongAdder();
        final LongAdder failed = new LongAdder();
        final LongAdder slow = new LongAdder();
        final LongAdder abnormal = new LongAdder();
        final LongAdder probeRounds = new LongAdder();
        final LongAdder probeSlowRounds = new LongAdder();
        final AtomicLong maxExecNanos = new AtomicLong();
        final AtomicLong maxEndToEndNanos = new AtomicLong();

        Bucket(long startNanos) {
            this.startNanos = startNanos;
        }
    }

    static final class TenantHealth {
        final AtomicReferenceArray<Bucket> buckets = new AtomicReferenceArray<>(BUCKETS);
        final ConcurrentHashMap<String, FileState> files = new ConcurrentHashMap<>();
    }

    static class FileState {
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
            return IoHealthSnapshot.FileStat.of(
                    tenant, path, consecutiveFails, fails, lastTouched);
        }
    }
}
