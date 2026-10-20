package com.ctrip.xpipe.redis.keeper.storage;

import com.ctrip.xpipe.redis.core.store.ReplId;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** keeper-tfs-m6 §6 */
public class AsyncFileSystemHelperReadTimeoutTest {

    private static final ReplId REPL_ID = ReplId.from(9L);
    private static final long TIMEOUT_MILLIS = 50L;

    private Path tempDir;
    private ExecutorService ioExecutor;
    private HangingReadDelegate delegate;
    private TailCacheFileSystem tcf;
    private DeferredHandleCloser closer;

    @After
    public void tearDown() {
        if (delegate != null) {
            delegate.release();
        }
        if (closer != null) {
            closer.shutdown(0);
        }
        if (tcf != null) {
            tcf.shutdown();
        }
        if (ioExecutor != null) {
            ioExecutor.shutdownNow();
        }
        if (tempDir != null) {
            deleteRecursively(tempDir.toFile());
        }
    }

    private static AsyncFile file(String path, AbstractStorageFile.OpenMode mode) {
        String key = StorageUtil.asyncFileKey(path);
        return new AsyncFile(path, AbstractStorageFile.ReplaceMode.NORMAL, mode, key, key, false);
    }

    // ---- mock FS ----

    @Test
    public void testTimedOutReadIsNotCancelledAndLateBufferIsReleased() throws Exception {
        AsyncFileSystem fs = mock(AsyncFileSystem.class);
        AsyncFile reader = file("/data/repl-9/meta.v2.json", AbstractStorageFile.OpenMode.READ);
        CompletableFuture<ByteBuf> readFuture = new CompletableFuture<>();
        when(fs.read(any(AsyncFile.class), anyLong(), anyLong())).thenReturn(readFuture);

        try {
            AsyncFileSystemHelper.runWithIoTimeout(TIMEOUT_MILLIS,
                    () -> AsyncFileSystemHelper.awaitRead(() -> fs.read(reader, 16, 0), "read meta"));
            fail("read must time out");
        } catch (IOException e) {
            assertTrue(e.getMessage().contains("timeout"));
        }
        assertFalse("read future must not be cancelled", readFuture.isCancelled());

        ByteBuf late = Unpooled.directBuffer(16).writeZero(16);
        readFuture.complete(late);
        assertEquals("late buffer released", 0, late.refCnt());
    }

    @Test
    public void testInterruptedReadIsNotCancelledAndLateBufferIsReleased() {
        AsyncFileSystem fs = mock(AsyncFileSystem.class);
        AsyncFile reader = file("/data/repl-9/cmd", AbstractStorageFile.OpenMode.READ);
        CompletableFuture<ByteBuf> readFuture = new CompletableFuture<>();
        when(fs.read(any(AsyncFile.class), anyLong(), anyLong())).thenReturn(readFuture);

        Thread.currentThread().interrupt();
        try {
            AsyncFileSystemHelper.awaitRead(() -> fs.read(reader, 8, 0), "read interrupted");
            fail("read must fail on interrupt");
        } catch (IOException e) {
            assertTrue(e.getMessage().contains("interrupted"));
        } finally {
            assertTrue("interrupt flag restored", Thread.interrupted());
        }
        assertFalse(readFuture.isCancelled());

        ByteBuf late = Unpooled.directBuffer(8).writeZero(8);
        readFuture.complete(late);
        assertEquals(0, late.refCnt());
    }

    @Test
    public void testReadAllBytesTimeoutReleasesLateBuffer() throws Exception {
        AsyncFileSystem fs = mock(AsyncFileSystem.class);
        AsyncFile reader = file("/data/repl-9/gtid.idx", AbstractStorageFile.OpenMode.READ);
        CompletableFuture<ByteBuf> readFuture = new CompletableFuture<>();
        when(fs.read(any(AsyncFile.class), anyLong(), anyLong())).thenReturn(readFuture);

        try {
            AsyncFileSystemHelper.runWithIoTimeout(TIMEOUT_MILLIS,
                    () -> AsyncFileSystemHelper.readAllBytes(fs, reader, 4, 0, "read all"));
            fail("read must time out");
        } catch (IOException expected) {
        }
        assertFalse(readFuture.isCancelled());
        ByteBuf late = Unpooled.directBuffer(4).writeZero(4);
        readFuture.complete(late);
        assertEquals(0, late.refCnt());
    }

    @Test
    public void testReadSuccessAndLateFailureAreUnaffected() throws Exception {
        AsyncFileSystem fs = mock(AsyncFileSystem.class);
        AsyncFile reader = file("/data/repl-9/ok", AbstractStorageFile.OpenMode.READ);
        ByteBuf data = Unpooled.directBuffer(4).writeZero(4);
        when(fs.read(reader, 4, 0)).thenReturn(CompletableFuture.completedFuture(data));

        ByteBuf got = AsyncFileSystemHelper.awaitRead(() -> fs.read(reader, 4, 0), "read ok");
        assertSame(data, got);
        assertEquals("caller owns the buffer on success", 1, got.refCnt());
        got.release();

        AsyncFile failing = file("/data/repl-9/late-fail", AbstractStorageFile.OpenMode.READ);
        CompletableFuture<ByteBuf> readFuture = new CompletableFuture<>();
        when(fs.read(failing, 4, 0)).thenReturn(readFuture);
        try {
            AsyncFileSystemHelper.runWithIoTimeout(TIMEOUT_MILLIS,
                    () -> AsyncFileSystemHelper.awaitRead(() -> fs.read(failing, 4, 0), "read late fail"));
            fail("read must time out");
        } catch (IOException expected) {
        }
        assertTrue(readFuture.completeExceptionally(new IOException("late EIO")));
    }

    @Test
    public void testWriteTimeoutDoesNotCancel() throws Exception {
        AsyncFileSystem fs = mock(AsyncFileSystem.class);
        AsyncFile writer = file("/data/repl-9/meta.v2.json", AbstractStorageFile.OpenMode.WRITE);
        CompletableFuture<Long> writeFuture = new CompletableFuture<>();
        when(fs.write(any(AsyncFile.class), any(ByteBuf.class))).thenReturn(writeFuture);

        try {
            AsyncFileSystemHelper.runWithIoTimeout(TIMEOUT_MILLIS,
                    () -> AsyncFileSystemHelper.writeAllBytes(fs, writer, new byte[4], "write meta"));
            fail("write must time out");
        } catch (IOException expected) {
        }
        assertFalse("write-type await does not cancel either", writeFuture.isCancelled());
    }

    // ---- real TailCache: read hangs on disk ----

    @Test
    public void testReadTimeoutThenCloseIsDeferredAndEverythingIsReleased() throws Exception {
        newTailCache();
        AtomicLong clock = new AtomicLong(1_000_000L);
        DeferredHandleCloserTest.ManualScheduler scheduler = new DeferredHandleCloserTest.ManualScheduler(clock);
        closer = new DeferredHandleCloser(tcf, () -> 60_000L, clock::get, scheduler.executor);
        DeferredHandleCloser.register(tcf, closer);

        Path p = tempDir.resolve("rdb_read");
        Files.write(p, new byte[256]);
        AsyncFile reader = tcf.open(p.toString(), AbstractStorageFile.OpenMode.READ,
                AbstractStorageFile.ReplaceMode.NORMAL, false, null).get(5, TimeUnit.SECONDS);

        delegate.hangReads();
        try {
            AsyncFileSystemHelper.runWithIoTimeout(TIMEOUT_MILLIS,
                    () -> AsyncFileSystemHelper.awaitRead(() -> tcf.read(reader, 64, 0), "read rdb"));
            fail("read must time out while the disk hangs");
        } catch (IOException e) {
            assertTrue(e.getMessage().contains("timeout"));
        }
        assertTrue("read reached the disk", delegate.readEntered.await(5, TimeUnit.SECONDS));

        // Read still in flight: close fails and goes to the closer.
        AsyncFileSystemHelper.closeReadHandle(tcf, reader, REPL_ID, "close rdb read");
        assertTrue(closer.isPending(reader));
        assertFalse(reader.closed);

        delegate.release();
        ByteBuf late = waitForLateBuffer();
        waitUntil(() -> late.refCnt() == 0);

        // In-flight cleanup may lag the release; retry a few times.
        for (int i = 0; i < 5 && closer.count() > 0; i++) {
            scheduler.runNext();
        }
        assertEquals(0, closer.count());
        assertTrue(reader.closed);
        assertEquals("tracker committed returned", 0, tcf.getGlobalCommittedBytes());
    }

    private ByteBuf waitForLateBuffer() throws InterruptedException {
        waitUntil(() -> delegate.lastRead.get() != null);
        return delegate.lastRead.get();
    }

    private void newTailCache() throws IOException {
        tempDir = Files.createTempDirectory("helper-read-timeout-test-");
        ioExecutor = Executors.newCachedThreadPool();
        delegate = new HangingReadDelegate(ioExecutor);
        TailCacheFileSystemConfig config = new TailCacheFileSystemConfig()
                // Force disk reads so the hang is hit.
                .setReadPreferCache(false)
                .setPerFileCacheLimits(10 * 1024, 1, 64)
                .setMaxCacheSizeBytes(100 * 1024)
                .setIoWaitTimeoutMs(200)
                .setRestoreWaitTimeoutMs(200);
        tcf = new TailCacheFileSystem(delegate, config, ioExecutor);
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

    /** Armed file reads block until released; the returned buffer is kept for refCnt checks. */
    static final class HangingReadDelegate extends TailCacheFileSystemTest.RecordingDelegate {
        private volatile CountDownLatch gate;
        final CountDownLatch readEntered = new CountDownLatch(1);
        final AtomicReference<ByteBuf> lastRead = new AtomicReference<>();

        HangingReadDelegate(ExecutorService ioExecutor) {
            super(ioExecutor);
        }

        void hangReads() {
            gate = new CountDownLatch(1);
        }

        void release() {
            CountDownLatch g = gate;
            gate = null;
            if (g != null) {
                g.countDown();
            }
        }

        @Override
        public ByteBuf readSync(AsyncFile file, long length, long offset, long alignSize) {
            CountDownLatch g = gate;
            if (g != null) {
                readEntered.countDown();
                try {
                    if (!g.await(30, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("read gate never released");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
                ByteBuf buf = super.readSync(file, length, offset, alignSize);
                lastRead.set(buf);
                return buf;
            }
            return super.readSync(file, length, offset, alignSize);
        }
    }
}
