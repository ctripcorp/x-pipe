package com.ctrip.xpipe.redis.keeper.storage.health;

import com.ctrip.xpipe.redis.keeper.storage.EIOException;
import com.ctrip.xpipe.redis.keeper.storage.OperationNotExecutedException;
import com.ctrip.xpipe.redis.keeper.storage.SocketErrorException;
import com.ctrip.xpipe.redis.keeper.storage.StaleStateException;
import com.ctrip.xpipe.redis.keeper.storage.StorageIOException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * The counters behind {@code TfsHealthChecker}: window arithmetic, the per-file state machine and
 * the per-tenant windows.
 */
public class IoTelemetryTest {

    private static final long BASE = TimeUnit.HOURS.toNanos(1);

    private IoTelemetry telemetry;
    private long now;

    @Before
    public void setUp() {
        telemetry = new IoTelemetry();
        now = BASE;
        telemetry.clock = () -> now;
    }

    @Test
    public void testDisabledSettingsRecordNothing() {
        // non-TFS containers: the pool is not measured at all
        telemetry.applySettings(new IoTelemetrySettings() {
            @Override
            public boolean enabled() {
                return false;
            }

            @Override
            public long slowNanos() {
                return IoTelemetrySettings.DEFAULTS.slowNanos();
            }

            @Override
            public long windowNanos() {
                return IoTelemetrySettings.DEFAULTS.windowNanos();
            }

            @Override
            public long fileWindowNanos() {
                return IoTelemetrySettings.DEFAULTS.fileWindowNanos();
            }

            @Override
            public long stateIdleNanos() {
                return IoTelemetrySettings.DEFAULTS.stateIdleNanos();
            }
        });

        record("repl_1", "/data/repl", true);

        IoHealthSnapshot snapshot = telemetry.snapshot(now);
        Assert.assertEquals(0L, snapshot.total());
        Assert.assertTrue(snapshot.tenants().isEmpty());
        Assert.assertEquals(0L, telemetry.recordedCount());
        Assert.assertEquals(0, telemetry.executingCount());
    }

    @Test
    public void testHighThroughputIsNotTruncated() {
        // far past the old 32768-entry ring: every task in the window still counts
        for (int i = 0; i < 100_000; i++) {
            record("repl_1", "/data/repl", true);
        }

        IoHealthSnapshot snapshot = telemetry.snapshot(now);

        Assert.assertEquals(100_000L, snapshot.total());
        Assert.assertEquals(100_000L, snapshot.failed());
        Assert.assertEquals(100_000L, snapshot.tenant("repl_1").total());
        Assert.assertEquals(100_000L, snapshot.tenant("repl_1").abnormal());
    }

    @Test
    public void testRecordLeavesTheFixedThirtySecondWindow() {
        // the window is fixed, not grown to chase the 1000-record floor: "30s没到1000就不判断" means
        // too-few-records is reported as insufficient, not compensated for by scanning further back
        record("repl_1", "/data/repl", true);

        now += TimeUnit.SECONDS.toNanos(11);
        IoHealthSnapshot snapshot = telemetry.snapshot(now);
        Assert.assertEquals(1L, snapshot.total());
        Assert.assertEquals(1L, snapshot.failed());

        now += TimeUnit.SECONDS.toNanos(20);
        snapshot = telemetry.snapshot(now);
        Assert.assertEquals(0L, snapshot.total());
    }

    @Test
    public void testLowThroughputStillReportsTheFullWindow() {
        // the lowest confirmed real throughput: 2 tasks/s, i.e. ~60 records in 30s. The window
        // still stops at exactly 30s and reports exactly what it saw; whether that is enough to
        // judge is the rule layer's decision (see FailRatioRule), not the counter's
        for (int i = 0; i < 60; i++) {
            record("repl_1", "/data/repl", false);
            if (i < 59) {
                // stay just under 30s: the 60th record must not fall exactly on the window's
                // one-second-granularity boundary, or it is the earliest bucket cut off
                now += TimeUnit.MILLISECONDS.toNanos(500);
            }
        }

        IoHealthSnapshot snapshot = telemetry.snapshot(now);
        Assert.assertEquals(60L, snapshot.total());
        Assert.assertEquals(0L, snapshot.failed());
    }

    @Test
    public void testWindowHoldsAThousandRecords() {
        for (int i = 0; i < 1000; i++) {
            record("repl_1", "/data/repl", false);
        }

        IoHealthSnapshot snapshot = telemetry.snapshot(now);
        Assert.assertEquals(1000L, snapshot.total());
    }

    @Test
    public void testConsecutiveFailuresFollowCompletionOrder() {
        // submitted in this order, but the success completes first: the run is broken by the
        // success and then restarted, so the trailing run is 1
        IoTask<Void> fail = new IoTask<>("repl_1", "/data/x", now, () -> null);
        IoTask<Void> ok = new IoTask<>("repl_1", "/data/x", now, () -> null);
        telemetry.taskStarted(ok);
        telemetry.taskCompleted(ok, null);
        telemetry.taskStarted(fail);
        telemetry.taskCompleted(fail, eio());

        IoHealthSnapshot.FileStat stat = file(telemetry.snapshot(now), "repl_1", "/data/x");
        Assert.assertEquals(1L, stat.consecutiveFails());
        Assert.assertEquals(1L, stat.fails10());
    }

    @Test
    public void testSuccessResetsTheConsecutiveFailures() {
        record("repl_1", "/data/x", true);
        record("repl_1", "/data/x", true);
        record("repl_1", "/data/x", false);
        record("repl_1", "/data/x", true);

        IoHealthSnapshot.FileStat stat = file(telemetry.snapshot(now), "repl_1", "/data/x");
        Assert.assertEquals(1L, stat.consecutiveFails());
        // the window rule still sees all three failures
        Assert.assertEquals(3L, stat.fails10());
    }

    @Test
    public void testFailuresInTheWindowCountedWithoutBeingConsecutive() {
        for (int i = 0; i < 5; i++) {
            record("repl_1", "/data/x", true);
            record("repl_1", "/data/x", false);
        }

        IoHealthSnapshot.FileStat stat = file(telemetry.snapshot(now), "repl_1", "/data/x");
        // every run was broken by a success, so only the window rule can catch it
        Assert.assertEquals(0L, stat.consecutiveFails());
        Assert.assertEquals(5L, stat.fails10());
    }

    @Test
    public void testFileStateIsDroppedOnceIdle() {
        record("repl_1", "/data/x", true);

        now += TimeUnit.MINUTES.toNanos(2);

        // the idle tenant, and its files with it, are gone
        Assert.assertNull(telemetry.snapshot(now).tenant("repl_1"));
    }

    @Test
    public void testIgnoredErrorIsNotAFailure() {
        IoTask<Void> task = new IoTask<>("repl_1", "/data/x", now, () -> null);
        telemetry.taskStarted(task);
        telemetry.taskCompleted(task, new RejectedExecutionException("shutdown"));

        IoHealthSnapshot snapshot = telemetry.snapshot(now);
        Assert.assertEquals(1L, snapshot.total());
        Assert.assertEquals(0L, snapshot.failed());
        Assert.assertEquals(0L, snapshot.tenant("repl_1").abnormal());
    }

    @Test
    public void testOnlyStorageErrorsCountAsAFailure() {
        // not the disk's fault: counted in total, never as a failure
        // a replica hung up mid-transfer
        record("repl_1", "/data/x", new SocketErrorException(new IOException("Broken pipe")));
        // file not found / already exists / directory not empty: the caller rebuilds and retries
        record("repl_1", "/data/x", new StaleStateException(new NoSuchFileException("/data/x")));
        // the caller gave up waiting; the task keeps running and is classified when it finishes
        record("repl_1", "/data/x", new OperationNotExecutedException("/data/x"));
        // a path that is not a directory
        record("repl_1", "/data/x", new IllegalArgumentException(new NotDirectoryException("/data/x")));

        // the storage itself failing
        record("repl_1", "/data/x", eio());
        record("repl_1", "/data/x", new StorageIOException("No space left on device"));

        IoHealthSnapshot snapshot = telemetry.snapshot(now);
        Assert.assertEquals(6L, snapshot.total());
        Assert.assertEquals(2L, snapshot.failed());
        Assert.assertEquals(2L, snapshot.tenant("repl_1").abnormal());
        Assert.assertEquals(2L, file(snapshot, "repl_1", "/data/x").consecutiveFails());
    }

    @Test
    public void testFileNotFoundIsNotAFailure() {
        // StorageUtil.wrapIOException maps NoSuchFileException to StaleStateException; a moved path
        // must never degrade a keeper
        record("repl_1", "/data/x", new StaleStateException(new NoSuchFileException("/data/x")));

        IoHealthSnapshot snapshot = telemetry.snapshot(now);
        Assert.assertEquals(1L, snapshot.total());
        Assert.assertEquals(0L, snapshot.failed());
        Assert.assertEquals(0L, snapshot.tenant("repl_1").abnormal());
        Assert.assertEquals(0L, file(snapshot, "repl_1", "/data/x").consecutiveFails());
    }

    @Test
    public void testWrappedEioStillCounts() {
        record("repl_1", "/data/x", new CompletionException(eio()));
        record("repl_1", "/data/x", new ExecutionException(eio()));
        record("repl_1", "/data/x", new CompletionException(new ExecutionException(eio())));

        Assert.assertEquals(3L, telemetry.snapshot(now).failed());
    }

    @Test
    public void testClosedChannelIsNotAFailure() {
        // wrapIOException maps a closed file channel to StorageIOException (a closed socket becomes
        // SocketErrorException), so this is our own close or shutdown, not the storage failing
        record("repl_1", "/data/x",
                new StorageIOException(new java.nio.channels.ClosedChannelException()));
        record("repl_1", "/data/x",
                new CompletionException(new StorageIOException(new java.nio.channels.ClosedChannelException())));

        IoHealthSnapshot snapshot = telemetry.snapshot(now);
        Assert.assertEquals(2L, snapshot.total());
        Assert.assertEquals(0L, snapshot.failed());
        Assert.assertEquals(0L, file(snapshot, "repl_1", "/data/x").consecutiveFails());
    }

    @Test
    public void testStorageIoErrorStillCounts() {
        // ENOSPC keeps counting: a full device is a device failure
        record("repl_1", "/data/x", new StorageIOException(new IOException("No space left on device")));

        Assert.assertEquals(1L, telemetry.snapshot(now).failed());
    }

    @Test
    public void testSlowTaskCountsAsAbnormalButNotAsFailed() {
        IoTask<Void> task = new IoTask<>("repl_1", "/data/x", now, () -> null);
        telemetry.taskStarted(task);
        now += TimeUnit.SECONDS.toNanos(3);
        telemetry.taskCompleted(task, null);

        IoHealthSnapshot snapshot = telemetry.snapshot(now);
        Assert.assertEquals(0L, snapshot.failed());
        Assert.assertEquals(1L, snapshot.slow());
        Assert.assertEquals(1L, snapshot.tenant("repl_1").abnormal());
        Assert.assertTrue(snapshot.maxExecNanos() >= TimeUnit.SECONDS.toNanos(3));
    }

    @Test
    public void testUnfinishedTaskShowsUpAsInFlight() {
        IoTask<Void> task = new IoTask<>("repl_1", "/data/x", now, () -> null);
        telemetry.taskStarted(task);

        now += TimeUnit.SECONDS.toNanos(6);

        IoHealthSnapshot snapshot = telemetry.snapshot(now);
        Assert.assertEquals(TimeUnit.SECONDS.toNanos(6), snapshot.maxInFlightNanos());
        Assert.assertEquals(0L, snapshot.total());
    }

    @Test
    public void testFinishDropsTheTaskFromExecuting() {
        IoTask<Void> running = new IoTask<>("repl_1", "/data/x", now, () -> null);
        IoTask<Void> done = new IoTask<>("repl_1", "/data/y", now, () -> null);
        telemetry.taskStarted(running);
        telemetry.taskStarted(done);
        Assert.assertEquals(2, telemetry.executingCount());

        telemetry.taskCompleted(done, null);

        // only tasks inside run() are tracked: the queue depth never shows up here
        Assert.assertEquals(1, telemetry.executingCount());
    }

    @Test
    public void testQueuedTaskIsNotAged() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(1);
        try {
            // keep the single worker busy so the next task has to wait in the queue
            CountDownLatch busy = new CountDownLatch(1);
            CountDownLatch started = new CountDownLatch(1);
            pool.execute(() -> {
                started.countDown();
                try {
                    busy.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            Assert.assertTrue(started.await(5, TimeUnit.SECONDS));

            // a task stuck in the queue is deliberately invisible to the hang rule
            IoTask<Void> queued = new IoTask<>("repl_1", "/data/x", now, () -> null);
            pool.execute(queued);
            now += TimeUnit.SECONDS.toNanos(6);

            Assert.assertEquals(0, telemetry.executingCount());
            Assert.assertEquals(0L, telemetry.snapshot(now).maxInFlightNanos());

            busy.countDown();
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    public void testInFlightCoversOnlyRunningTasks() {
        IoTask<Void> task = new IoTask<>("repl_1", "/data/x", now, () -> null);
        telemetry.taskStarted(task);
        now += TimeUnit.SECONDS.toNanos(4);

        Assert.assertEquals(TimeUnit.SECONDS.toNanos(4), telemetry.snapshot(now).maxInFlightNanos());
    }

    @Test
    public void testInFlightAgeExcludesTheQueueWait() {
        // queued for 10s, then running for 1s: the execution time so far is 1s
        IoTask<Void> task = new IoTask<>("repl_1", "/data/x", now, () -> null);
        now += TimeUnit.SECONDS.toNanos(10);
        telemetry.taskStarted(task);
        now += TimeUnit.SECONDS.toNanos(1);

        IoHealthSnapshot snapshot = telemetry.snapshot(now);
        Assert.assertEquals(TimeUnit.SECONDS.toNanos(1), snapshot.maxInFlightNanos());
        Assert.assertEquals(1L, snapshot.inFlight());
        Assert.assertEquals(0L, snapshot.slowInFlight());
    }

    @Test
    public void testFinishedExecTimeExcludesTheQueueWait() {
        IoTask<Void> task = new IoTask<>("repl_1", "/data/x", now, () -> null);
        now += TimeUnit.SECONDS.toNanos(10);
        telemetry.taskStarted(task);
        now += TimeUnit.SECONDS.toNanos(1);
        telemetry.taskCompleted(task, null);

        IoHealthSnapshot snapshot = telemetry.snapshot(now);
        Assert.assertEquals(TimeUnit.SECONDS.toNanos(1), snapshot.maxExecNanos());
        Assert.assertEquals(0L, snapshot.slow());
        Assert.assertEquals(0L, snapshot.tenant("repl_1").abnormal());
    }

    @Test
    public void testRunningTaskAboveTwoSecondsIsSlowInFlight() {
        IoTask<Void> slow = new IoTask<>("repl_1", "/data/x", now, () -> null);
        IoTask<Void> fast = new IoTask<>("repl_1", "/data/y", now, () -> null);
        telemetry.taskStarted(slow);
        now += TimeUnit.SECONDS.toNanos(2);
        telemetry.taskStarted(fast);
        now += TimeUnit.SECONDS.toNanos(1);

        IoHealthSnapshot snapshot = telemetry.snapshot(now);
        Assert.assertEquals(2L, snapshot.inFlight());
        Assert.assertEquals(1L, snapshot.slowInFlight());
        // running tasks are not part of the finished-task window
        Assert.assertEquals(0L, snapshot.total());
        IoHealthSnapshot.TenantStat tenant = snapshot.tenant("repl_1");
        Assert.assertNotNull(tenant);
        Assert.assertEquals(2L, tenant.inFlight());
        Assert.assertEquals(1L, tenant.slowInFlight());
        Assert.assertEquals(0L, tenant.total());
    }

    private IoHealthSnapshot.FileStat file(IoHealthSnapshot snapshot, String tenant, String path) {
        IoHealthSnapshot.TenantStat tenantStat = snapshot.tenant(tenant);
        Assert.assertNotNull(tenantStat);
        IoHealthSnapshot.FileStat stat = tenantStat.files().get(path);
        Assert.assertNotNull(stat);
        return stat;
    }

    private void record(String tenant, String path, boolean failed) {
        record(tenant, path, failed ? eio() : null);
    }

    private void record(String tenant, String path, Throwable error) {
        long submittedAt = now;
        IoTask<Void> task = new IoTask<>(tenant, path, submittedAt, () -> null);
        telemetry.taskStarted(task);
        telemetry.taskCompleted(task, error);
    }

    private static EIOException eio() {
        return new EIOException(new IOException("Input/output error"));
    }
}
