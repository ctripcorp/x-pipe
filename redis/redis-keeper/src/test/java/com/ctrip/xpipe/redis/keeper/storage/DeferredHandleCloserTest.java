package com.ctrip.xpipe.redis.keeper.storage;

import com.ctrip.xpipe.redis.core.store.ReplId;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * keeper-tfs-m6 P1 (spec §2.1, §2.3): DeferredHandleCloser scheduling, retry and indices.
 */
public class DeferredHandleCloserTest {

    private static final long WINDOW_MILLIS = 20_000L;
    private static final ReplId REPL_ID = ReplId.from(1L);

    private final AtomicLong clock = new AtomicLong(1_000_000L);
    private ManualScheduler scheduler;
    private AsyncFileSystem fs;
    // Close outcomes in call order; empty means success. Each entry is thrown by the close call.
    private final Deque<Throwable> closeFailures = new ArrayDeque<>();
    private final List<Boolean> closeNoFlush = new ArrayList<>();
    private DeferredHandleCloser closer;

    private Path tempDir;
    private TrackingExecutor ioExecutor;
    private TailCacheFileSystemTest.FaultyDelegate faulty;
    private TailCacheFileSystem tcf;

    @Before
    public void setUp() {
        scheduler = new ManualScheduler(clock);
        fs = mock(AsyncFileSystem.class);
        when(fs.close(any(AsyncFile.class), anyBoolean())).thenAnswer(inv -> recordClose(inv.getArgument(1)));
        when(fs.close(any(AsyncSegmentFile.class), anyBoolean())).thenAnswer(inv -> recordClose(inv.getArgument(1)));
        closer = new DeferredHandleCloser(fs, () -> WINDOW_MILLIS, clock::get, scheduler.executor);
    }

    @After
    public void tearDown() {
        if (faulty != null) {
            faulty.release();
        }
        if (tcf != null) {
            tcf.shutdown();
        }
        if (tempDir != null) {
            deleteRecursively(tempDir.toFile());
        }
    }

    private CompletableFuture<Void> recordClose(boolean noFlush) throws Throwable {
        closeNoFlush.add(noFlush);
        Throwable failure = closeFailures.poll();
        if (failure != null) {
            throw failure;
        }
        return CompletableFuture.completedFuture(null);
    }

    private void failNextCloses(int times) {
        for (int i = 0; i < times; i++) {
            closeFailures.add(new OperationNotExecutedException("injected-" + i));
        }
    }

    private static AsyncFile file(String path, AbstractStorageFile.OpenMode mode) {
        String key = StorageUtil.asyncFileKey(path);
        return new AsyncFile(path, AbstractStorageFile.ReplaceMode.NORMAL, mode, key, key, false);
    }

    // ---- close mode ----

    @Test
    public void testWriteHandleFlushesUntilDeadlineThenNoFlushForever() {
        AsyncFile writer = file("/data/repl-1/meta.v2.json", AbstractStorageFile.OpenMode.WRITE);
        long deferredAt = clock.get();
        closer.defer(writer, REPL_ID, "close meta", new OperationNotExecutedException("helper"));
        DeferredHandle handle = closer.findPending(writer.getKey());
        assertEquals(REPL_ID, handle.getReplId());
        assertTrue(handle.isWrite());
        assertEquals(deferredAt + WINDOW_MILLIS, handle.getFlushDeadlineMillis());
        assertEquals(DeferredHandle.CloseMode.FLUSH, handle.currentMode());

        // Retries at +1s, +3s, +7s, +15s are inside the window; +31s is past it.
        failNextCloses(7);
        for (int i = 0; i < 7; i++) {
            scheduler.runNext();
        }
        assertEquals(Arrays.asList(false, false, false, false, true, true, true), closeNoFlush);
        assertTrue(handle.isDegraded());
        assertEquals(DeferredHandle.CloseMode.NO_FLUSH, handle.currentMode());

        scheduler.runNext();
        assertEquals("degraded handle never goes back to flush", Boolean.TRUE, last(closeNoFlush));
        assertFalse(closer.isPending(writer));
        assertEquals(0, closer.count());
    }

    @Test
    public void testReadHandleAlwaysClosesWithoutFlush() {
        AsyncFile reader = file("/data/repl-1/rdb", AbstractStorageFile.OpenMode.READ);
        closer.defer(reader, REPL_ID, "close rdb reader", new OperationNotExecutedException("helper"));
        DeferredHandle handle = closer.findPending(reader.getKey());
        assertFalse(handle.isWrite());
        assertEquals(0L, handle.getFlushDeadlineMillis());
        assertEquals(DeferredHandle.CloseMode.NO_FLUSH, handle.currentMode());

        failNextCloses(2);
        scheduler.runNext();
        scheduler.runNext();
        scheduler.runNext();
        assertEquals(Arrays.asList(true, true, true), closeNoFlush);
        assertFalse("read handle is never degraded, it never flushed", handle.isDegraded());
        assertEquals(0, closer.count());
    }

    @Test
    public void testSegmentHandleIsClosedThroughSegmentApi() {
        String key = StorageUtil.segmentKey("/data/repl-1", "cmd_");
        AsyncSegmentFile segment = new AsyncSegmentFile("/data/repl-1", "cmd_", Collections.emptyList(),
                key, key, true);
        closer.defer(segment, REPL_ID, "close cmd", new OperationNotExecutedException("helper"));
        assertEquals("s|/data/repl-1|cmd_", closer.findPending(key).getDisplayKey());
        scheduler.runNext();
        assertEquals(Collections.singletonList(false), closeNoFlush);
        assertEquals(0, closer.count());
    }

    // ---- backoff ----

    @Test
    public void testBackoffDoublesAndCapsAt30s() {
        long[] expected = {1000, 2000, 4000, 8000, 16000, 30000, 30000};
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], DeferredHandleCloser.backoffMillis(i + 1));
        }
        assertEquals("no shift overflow", 30000, DeferredHandleCloser.backoffMillis(Integer.MAX_VALUE));
        assertEquals(1000, DeferredHandleCloser.backoffMillis(0));
    }

    @Test
    public void testBackoffResetsWhenDegradedAndCountsFromAttemptEnd() {
        AsyncFile writer = file("/data/repl-1/manager.meta", AbstractStorageFile.OpenMode.WRITE);
        closer.defer(writer, REPL_ID, "close manager meta", new OperationNotExecutedException("helper"));
        DeferredHandle handle = closer.findPending(writer.getKey());
        // A close that takes 500ms: the next retry is counted from its end.
        // doAnswer: re-stubbing with when() would invoke the current answer and record a close.
        doAnswer(inv -> {
            clock.addAndGet(500);
            return recordClose(inv.getArgument(1));
        }).when(fs).close(any(AsyncFile.class), anyBoolean());

        failNextCloses(8);
        for (int i = 0; i < 8; i++) {
            scheduler.runNext();
        }
        // Flush attempts end at +1.5s, +4s, +8.5s, +17s; the one starting at +33s degrades.
        assertEquals(Arrays.asList(1000L, 2000L, 4000L, 8000L, 16000L, 1000L, 2000L, 4000L, 8000L),
                scheduler.delays);
        assertEquals(Arrays.asList(false, false, false, false, true, true, true, true), closeNoFlush);
        assertEquals(8, handle.getAttempts());
        assertEquals(clock.get() + 8000L, handle.getNextRetryTimeMillis());
        assertTrue(handle.getLastError() instanceof OperationNotExecutedException);
    }

    // ---- indices ----

    @Test
    public void testIndicesKeepDistinctObjectsOfOneKeyAndIgnoreDuplicates() {
        String path = "/data/repl-1/rdb_1";
        AsyncFile reader = file(path, AbstractStorageFile.OpenMode.READ);
        AsyncFile writer = file(path, AbstractStorageFile.OpenMode.WRITE);
        String key = writer.getKey();

        assertTrue(closer.defer(reader, REPL_ID, "close reader", new OperationNotExecutedException("r")));
        assertTrue(closer.defer(writer, REPL_ID, "close writer", new OperationNotExecutedException("w")));
        assertTrue("re-defer of a pending object is ignored", closer.defer(writer, REPL_ID, "again", null));
        assertEquals(2, closer.count());
        assertEquals(1, closer.writeCount());
        assertEquals("duplicate is not scheduled", 2, scheduler.tasks.size());
        assertSame("write handle preferred", writer, closer.findPending(key).file);
        assertTrue(closer.isPending(reader));
        assertTrue(closer.isPending(writer));
        assertFalse("identity, not key", closer.isPending(file(path, AbstractStorageFile.OpenMode.WRITE)));

        // Reader retry fails and stays; writer closes.
        failNextCloses(1);
        scheduler.runNext();
        scheduler.runNext();
        assertTrue(closer.isPending(reader));
        assertFalse(closer.isPending(writer));
        assertSame(reader, closer.findPending(key).file);
        assertEquals(0, closer.writeCount());

        scheduler.runNext();
        assertFalse(closer.isPending(reader));
        assertNull(closer.findPending(key));
        assertEquals(0, closer.count());
        assertTrue(closer.snapshot().isEmpty());
    }

    @Test
    public void testSnapshotListsOldestFirst() {
        AsyncFile first = file("/data/repl-1/a", AbstractStorageFile.OpenMode.WRITE);
        AsyncFile second = file("/data/repl-2/b", AbstractStorageFile.OpenMode.READ);
        closer.defer(first, REPL_ID, "close a", null);
        clock.addAndGet(10);
        closer.defer(second, REPL_ID, "close b", null);
        List<DeferredHandle> snapshot = closer.snapshot();
        assertEquals(2, snapshot.size());
        assertSame(first, snapshot.get(0).file);
        assertSame(second, snapshot.get(1).file);
        try {
            snapshot.clear();
            fail("snapshot must be read-only");
        } catch (UnsupportedOperationException expected) {
            // read-only view
        }
    }

    // ---- terminal outcomes ----

    @Test
    public void testProgrammingErrorsAreDropped() {
        AsyncFile notAllowed = file("/data/repl-1/x", AbstractStorageFile.OpenMode.WRITE);
        AsyncFile badArg = file("/data/repl-1/y", AbstractStorageFile.OpenMode.WRITE);
        closer.defer(notAllowed, REPL_ID, "close x", null);
        closer.defer(badArg, REPL_ID, "close y", null);
        closeFailures.add(new IllegalStateException("close is not allowed for: x"));
        closeFailures.add(new IllegalArgumentException("bad"));
        scheduler.runNext();
        scheduler.runNext();
        assertEquals(0, closer.count());
        assertTrue("dropped handles are not rescheduled", scheduler.tasks.isEmpty());
    }

    @Test
    public void testProgrammingErrorFromFutureIsDropped() {
        AsyncFile writer = file("/data/repl-1/z", AbstractStorageFile.OpenMode.WRITE);
        CompletableFuture<Void> failed = new CompletableFuture<>();
        failed.completeExceptionally(new IllegalStateException("from future"));
        doReturn(failed).when(fs).close(any(AsyncFile.class), anyBoolean());
        closer.defer(writer, REPL_ID, "close z", null);
        scheduler.runNext();
        assertEquals(0, closer.count());
        assertTrue(scheduler.tasks.isEmpty());
    }

    @Test
    public void testThrowableKeepsHandleAndWorkerRunning() {
        AsyncFile writer = file("/data/repl-1/w", AbstractStorageFile.OpenMode.WRITE);
        closer.defer(writer, REPL_ID, "close w", null);
        closeFailures.add(new Error("boom"));
        closeFailures.add(new IOException("eio"));
        scheduler.runNext();
        scheduler.runNext();
        assertTrue("failed handle is never lost", closer.isPending(writer));
        assertEquals(2, closer.findPending(writer.getKey()).getAttempts());
        assertTrue(closer.findPending(writer.getKey()).getLastError() instanceof IOException);
        scheduler.runNext();
        assertEquals(0, closer.count());
    }

    @Test
    public void testRegistryIsIdentityBasedAndShutdownUnregisters() {
        assertNull(DeferredHandleCloser.of(fs));
        DeferredHandleCloser.register(fs, closer);
        assertSame(closer, DeferredHandleCloser.of(fs));
        assertNull(DeferredHandleCloser.of(mock(AsyncFileSystem.class)));
        try {
            DeferredHandleCloser.register(mock(AsyncFileSystem.class), closer);
            fail("closer bound to another FS");
        } catch (IllegalArgumentException expected) {
            // closer.fs mismatch
        }
        closer.shutdown(0);
        assertNull(DeferredHandleCloser.of(fs));
    }

    // ---- shutdown on a real worker thread ----

    @Test(timeout = 20_000)
    public void testShutdownDoesNotInterruptAndReportsLeftover() throws Exception {
        CountDownLatch closeEntered = new CountDownLatch(1);
        CountDownLatch releaseClose = new CountDownLatch(1);
        AtomicBoolean interrupted = new AtomicBoolean();
        AtomicInteger secondCloses = new AtomicInteger();
        AsyncFile blocking = file("/data/repl-1/blocking", AbstractStorageFile.OpenMode.WRITE);
        AsyncFile queued = file("/data/repl-1/queued", AbstractStorageFile.OpenMode.WRITE);
        AsyncFileSystem realFs = mock(AsyncFileSystem.class);
        when(realFs.close(any(AsyncFile.class), anyBoolean())).thenAnswer(inv -> {
            if (inv.getArgument(0) != blocking) {
                secondCloses.incrementAndGet();
                return CompletableFuture.completedFuture(null);
            }
            closeEntered.countDown();
            try {
                releaseClose.await();
            } catch (InterruptedException e) {
                interrupted.set(true);
            }
            interrupted.compareAndSet(false, Thread.currentThread().isInterrupted());
            return CompletableFuture.completedFuture(null);
        });
        DeferredHandleCloser real = new DeferredHandleCloser(realFs, () -> WINDOW_MILLIS);
        DeferredHandleCloser.register(realFs, real);

        real.defer(blocking, REPL_ID, "close blocking", null);
        assertTrue("first retry runs after 1s", closeEntered.await(5, TimeUnit.SECONDS));
        real.defer(queued, REPL_ID, "close queued", null);

        List<DeferredHandle> leftover = real.shutdown(200);
        assertEquals(2, leftover.size());
        assertNull(DeferredHandleCloser.of(realFs));
        assertFalse("stops accepting after shutdown",
                real.defer(file("/data/repl-1/late", AbstractStorageFile.OpenMode.WRITE), REPL_ID, "late", null));

        releaseClose.countDown();
        waitUntil(() -> !real.isPending(blocking));
        assertFalse("in-progress close is not interrupted", interrupted.get());
        Thread.sleep(1500);
        assertEquals("queued retry dropped by shutdown", 0, secondCloses.get());
        assertTrue(real.isPending(queued));
    }

    // ---- ReplId comes from the close caller ----

    @Test
    public void testReplIdIsTakenFromCallerAndNullShowsUnknown() {
        String path = "/data/shared/rdb";
        AsyncFile first = file(path, AbstractStorageFile.OpenMode.READ);
        AsyncFile second = file(path, AbstractStorageFile.OpenMode.READ);
        ReplId other = ReplId.from(2L);
        closer.defer(first, other, "close first", null);
        clock.addAndGet(10);
        closer.defer(second, null, "close second", null);
        List<DeferredHandle> snapshot = closer.snapshot();
        assertEquals(other, snapshot.get(0).getReplId());
        assertNull(snapshot.get(1).getReplId());
        assertTrue(snapshot.get(1).toString().contains("repl_id=" + DeferredHandle.UNKNOWN_REPL_ID));
    }

    // ---- P8 (spec §8): log content and rate limit ----

    @Test
    public void testEntryLogsCarryReplIdAndFileKeyAndNoStackForCloseFailures() {
        AsyncFile writer = file("/data/repl-1/meta.v2.json", AbstractStorageFile.OpenMode.WRITE);
        String entry = "repl_id=" + REPL_ID + " fileKey=f|/data/repl-1/meta.v2.json write=true";
        try (LogCapture logs = new LogCapture(DeferredHandleCloser.class)) {
            closer.defer(writer, REPL_ID, "close meta", new OperationNotExecutedException("helper"));
            // Fails at +1s, +3s, +7s, +15s (flush) and +31s (degraded); closes at +32s.
            failNextCloses(5);
            for (int i = 0; i < 6; i++) {
                scheduler.runNext();
            }
            assertEquals(0, closer.count());

            LogEvent deferred = single(logs, "[deferred-closer][deferred]");
            assertEquals(Level.WARN, deferred.getLevel());
            assertTrue(LogCapture.message(deferred), LogCapture.message(deferred).contains(entry));
            assertTrue(LogCapture.message(deferred).contains("cause: OperationNotExecutedException: operation was not executed for: helper"));
            assertNull("no stack for the deferred cause", deferred.getThrown());

            LogEvent retryFail = single(logs, "[deferred-closer][retry-fail]");
            assertEquals(Level.WARN, retryFail.getLevel());
            assertTrue(LogCapture.message(retryFail).contains(entry + " attempts=1 "));
            assertTrue(LogCapture.message(retryFail).contains("cause: OperationNotExecutedException: operation was not executed for: injected-0"));
            assertNull("no stack for retry failures", retryFail.getThrown());

            LogEvent degraded = single(logs, "[deferred-closer][degrade-no-flush]");
            assertEquals(Level.ERROR, degraded.getLevel());
            assertTrue(LogCapture.message(degraded).contains(entry + " attempts=4 age=31000ms mode=NO_FLUSH"));

            LogEvent closed = single(logs, "[deferred-closer][closed]");
            assertEquals(Level.INFO, closed.getLevel());
            assertTrue(LogCapture.message(closed).contains(entry + " attempts=5 age=32000ms mode=NO_FLUSH"));
        }
    }

    @Test
    public void testRetryFailureLogIsRateLimitedPerMinute() {
        AsyncFile reader = file("/data/repl-1/rdb", AbstractStorageFile.OpenMode.READ);
        try (LogCapture logs = new LogCapture(DeferredHandleCloser.class)) {
            closer.defer(reader, REPL_ID, "close rdb reader", new OperationNotExecutedException("helper"));
            // Failures at +1s, +3s, +7s, +15s, +31s, +61s, +91s, +121s, +151s, +181s.
            failNextCloses(10);
            for (int i = 0; i < 10; i++) {
                scheduler.runNext();
            }
            assertEquals(10, closer.findPending(reader.getKey()).getAttempts());
            List<String> logged = new ArrayList<>();
            for (LogEvent event : logs.withTag("[deferred-closer][retry-fail]")) {
                String message = LogCapture.message(event);
                logged.add(message.substring(message.indexOf("attempts="), message.indexOf(" age=")));
            }
            // +1s first, then the first failure at least 60s after the last logged one.
            assertEquals(Arrays.asList("attempts=1", "attempts=6", "attempts=8", "attempts=10"), logged);
        }
    }


    private static LogEvent single(LogCapture logs, String tag) {
        List<LogEvent> events = logs.withTag(tag);
        assertEquals("events tagged " + tag, 1, events.size());
        return events.get(0);
    }

    // ---- real TailCache: memory release ----

    @Test
    public void testDeferredCloseReleasesTailCacheMemory() throws Exception {
        newTailCache();
        DeferredHandleCloser tcfCloser = new DeferredHandleCloser(tcf, () -> WINDOW_MILLIS, clock::get,
                scheduler.executor);
        String p = tempDir.resolve("deferred_release").toString();
        AsyncFile writer = tcf.open(p, AbstractStorageFile.OpenMode.WRITE, AbstractStorageFile.ReplaceMode.NORMAL,
                false, null).get(5, TimeUnit.SECONDS);

        faulty.hangOn(TailCacheFileSystemTest.Op.FILE_WRITE);
        tcf.write(writer, Unpooled.wrappedBuffer(new byte[200])).get(5, TimeUnit.SECONDS);
        FileCacheEntry entry = writer.getCacheEntry();
        List<ByteBuf> pinned = new ArrayList<>();
        for (CacheChunk chunk : entry.chunks.values()) {
            pinned.add(chunk.buffer);
        }
        assertFalse(pinned.isEmpty());

        OperationNotExecutedException helperFailure = null;
        try {
            tcf.close(writer);
            fail("close must fail while the write is wedged");
        } catch (OperationNotExecutedException e) {
            helperFailure = e;
        }
        tcfCloser.defer(writer, REPL_ID, "close deferred_release", helperFailure);
        assertEquals(REPL_ID, tcfCloser.findPending(writer.getKey()).getReplId());
        assertTrue("memory pinned while pending", tcf.getGlobalCommittedBytes() > 0);

        faulty.release();
        scheduler.runNext();
        ioExecutor.awaitAll();

        assertEquals(0, tcfCloser.count());
        assertTrue(writer.closed);
        assertEquals(0, entry.refCount);
        assertTrue(entry.chunks.isEmpty());
        for (ByteBuf buf : pinned) {
            assertEquals("chunk buffer freed", 0, buf.refCnt());
        }
        assertEquals("tracker committed returned", 0, tcf.getGlobalCommittedBytes());
        assertEquals("flushed by the deferred close", 200, Files.size(java.nio.file.Paths.get(p)));
    }

    private void newTailCache() throws IOException {
        tempDir = Files.createTempDirectory("deferred-closer-test-");
        ioExecutor = new TrackingExecutor(Executors.newCachedThreadPool());
        faulty = new TailCacheFileSystemTest.FaultyDelegate(ioExecutor);
        TailCacheFileSystemConfig config = new TailCacheFileSystemConfig();
        config.setPerFileCacheLimits(10 * 1024, 1, 64);
        config.setMaxCacheSizeBytes(100 * 1024);
        config.setWriteBatchBytes(128);
        config.setIoWaitTimeoutMs(200);
        config.setRestoreWaitTimeoutMs(200);
        config.setExpectedMinRetentionMs(0);
        config.setEvictScanIntervalMs(60_000);
        config.setWatermarkRatios(0.5, 0.8);
        config.setMaxEvictRatioPerWrite(0.5);
        tcf = new TailCacheFileSystem(faulty, config, ioExecutor);
    }

    // ---- helpers ----

    private static <T> T last(List<T> list) {
        return list.get(list.size() - 1);
    }

    private static void waitUntil(java.util.function.BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5000;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                fail("condition not met within 5s");
            }
            Thread.sleep(20);
        }
    }

    private static void deleteRecursively(File f) {
        File[] children = f.listFiles();
        if (children != null) {
            for (File c : children) {
                deleteRecursively(c);
            }
        }
        f.delete();
    }

    /** Captures scheduled retries; runNext advances the clock by the task's delay and runs it. */
    static final class ManualScheduler {
        final ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        final Deque<Object[]> tasks = new ArrayDeque<>();
        final List<Long> delays = new ArrayList<>();
        private final AtomicLong clock;

        ManualScheduler(AtomicLong clock) {
            this.clock = clock;
            when(executor.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class))).thenAnswer(inv -> {
                long delayMillis = ((TimeUnit) inv.getArgument(2)).toMillis(inv.getArgument(1));
                tasks.add(new Object[]{inv.getArgument(0), delayMillis});
                delays.add(delayMillis);
                return null;
            });
        }

        void runNext() {
            Object[] task = tasks.poll();
            assertNotNull("no scheduled retry", task);
            clock.addAndGet((Long) task[1]);
            ((Runnable) task[0]).run();
        }
    }
}
