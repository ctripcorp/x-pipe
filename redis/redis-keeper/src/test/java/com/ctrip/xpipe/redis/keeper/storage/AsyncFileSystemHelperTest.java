package com.ctrip.xpipe.redis.keeper.storage;

import com.ctrip.xpipe.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Phase S / T-S.4 + Se T-S.8 / T-S.11 / T-S.17–S.19: awaitOpen abandon + write/read close split.
 * Phase H1 / T-H1.4: OperationNotExecutedException retry once at await entry.
 * Phase H3a / T-H3.CP-W.4 / .1: write sync-throw retry (any Exception) vs no retry after future.
 */
public class AsyncFileSystemHelperTest extends AbstractTest {

    @Test
    public void awaitOpenTimeoutAbandonsLateSuccessAndCloses() throws Exception {
        AsyncFileSystem fs = mock(AsyncFileSystem.class);
        AsyncFile lateFile = mock(AsyncFile.class);
        CountDownLatch closeCalled = new CountDownLatch(1);
        when(fs.close(any(AsyncFile.class))).thenAnswer(invocation -> {
            closeCalled.countDown();
            return CompletableFuture.completedFuture(null);
        });

        CompletableFuture<AsyncFile> openFuture = new CompletableFuture<>();
        AtomicReference<IOException> thrown = new AtomicReference<>();
        Thread waiter = new Thread(() -> {
            try {
                AsyncFileSystemHelper.runWithIoTimeout(50, () ->
                        AsyncFileSystemHelper.awaitOpen(fs, openFuture, "slow open"));
                Assert.fail("expected timeout");
            } catch (IOException e) {
                thrown.set(e);
            }
        });
        waiter.start();
        waiter.join(TimeUnit.SECONDS.toMillis(5));
        Assert.assertFalse(waiter.isAlive());
        Assert.assertNotNull(thrown.get());
        Assert.assertTrue(thrown.get().getMessage().contains("timeout"));
        Assert.assertFalse("open future must not be cancelled on timeout", openFuture.isCancelled());

        openFuture.complete(lateFile);
        Assert.assertTrue("late open should be closed", closeCalled.await(3, TimeUnit.SECONDS));
        verify(fs).close(lateFile);
    }

    @Test
    public void awaitOpenInterruptAbandonsLateSuccessAndCloses() throws Exception {
        AsyncFileSystem fs = mock(AsyncFileSystem.class);
        AsyncFile lateFile = mock(AsyncFile.class);
        CountDownLatch closeCalled = new CountDownLatch(1);
        when(fs.close(any(AsyncFile.class))).thenAnswer(invocation -> {
            closeCalled.countDown();
            return CompletableFuture.completedFuture(null);
        });

        CompletableFuture<AsyncFile> openFuture = new CompletableFuture<>();
        CountDownLatch enteredAwait = new CountDownLatch(1);
        AtomicReference<IOException> thrown = new AtomicReference<>();
        Thread waiter = new Thread(() -> {
            try {
                enteredAwait.countDown();
                AsyncFileSystemHelper.awaitOpen(fs, openFuture, "interrupted open");
                Assert.fail("expected interrupt IOException");
            } catch (IOException e) {
                thrown.set(e);
            }
        });
        waiter.start();
        Assert.assertTrue(enteredAwait.await(3, TimeUnit.SECONDS));
        // Give the waiter time to block in future.get
        sleep(50);
        waiter.interrupt();
        waiter.join(TimeUnit.SECONDS.toMillis(5));
        Assert.assertFalse(waiter.isAlive());
        Assert.assertNotNull(thrown.get());
        Assert.assertTrue(thrown.get().getMessage().contains("interrupted"));
        Assert.assertFalse("open future must not be cancelled on interrupt", openFuture.isCancelled());

        openFuture.complete(lateFile);
        Assert.assertTrue("late open should be closed after interrupt abandon",
                closeCalled.await(3, TimeUnit.SECONDS));
        verify(fs).close(lateFile);
    }

    @Test
    public void awaitOpenAbandonDoesNotAwaitSlowClose() throws Exception {
        AsyncFileSystem fs = mock(AsyncFileSystem.class);
        AsyncFile lateFile = mock(AsyncFile.class);
        CountDownLatch closeInvoked = new CountDownLatch(1);
        CompletableFuture<Void> neverDoneClose = new CompletableFuture<>();
        when(fs.close(any(AsyncFile.class))).thenAnswer(invocation -> {
            closeInvoked.countDown();
            return neverDoneClose;
        });

        CompletableFuture<AsyncFile> openFuture = new CompletableFuture<>();
        AtomicReference<IOException> thrown = new AtomicReference<>();
        Thread waiter = new Thread(() -> {
            try {
                AsyncFileSystemHelper.runWithIoTimeout(50, () ->
                        AsyncFileSystemHelper.awaitOpen(fs, openFuture, "abandon no await close"));
                Assert.fail("expected timeout");
            } catch (IOException e) {
                thrown.set(e);
            }
        });
        waiter.start();
        waiter.join(TimeUnit.SECONDS.toMillis(5));
        Assert.assertFalse(waiter.isAlive());
        Assert.assertNotNull(thrown.get());

        long beforeComplete = System.nanoTime();
        openFuture.complete(lateFile);
        Assert.assertTrue(closeInvoked.await(3, TimeUnit.SECONDS));
        // closeReadHandle path must return without waiting for neverDoneClose
        Assert.assertTrue("abandon must not block on close future",
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - beforeComplete) < 500);
        Assert.assertFalse(neverDoneClose.isCancelled());
        neverDoneClose.complete(null);
    }

    @Test
    public void awaitOpenReturnsHandleWhenCompletedInTime() throws Exception {
        AsyncFileSystem fs = mock(AsyncFileSystem.class);
        AsyncFile file = mock(AsyncFile.class);
        CompletableFuture<AsyncFile> openFuture = CompletableFuture.completedFuture(file);

        AsyncFile opened = AsyncFileSystemHelper.awaitOpen(fs, openFuture, "fast open");
        Assert.assertSame(file, opened);
    }

    @Test
    public void awaitTimeoutCancelsFuture() throws Exception {
        CompletableFuture<Long> future = new CompletableFuture<>();
        try {
            AsyncFileSystemHelper.runWithIoTimeout(50, () ->
                    AsyncFileSystemHelper.await(future, "slow size"));
            Assert.fail("expected timeout");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().contains("timeout"));
        }
        Assert.assertTrue(future.isCancelled());
    }

    @Test
    public void closeHandleRetriesOnceOnOperationNotExecutedThenSucceeds() {
        AsyncFileSystem fs = mock(AsyncFileSystem.class);
        AsyncFile file = mock(AsyncFile.class);
        AtomicInteger closeCalls = new AtomicInteger();
        when(fs.close(any(AsyncFile.class))).thenAnswer(invocation -> {
            if (closeCalls.getAndIncrement() == 0) {
                throw new OperationNotExecutedException("dirty");
            }
            return CompletableFuture.completedFuture(null);
        });

        AsyncFileSystemHelper.closeHandle(fs, file, "close retry ok");
        Assert.assertEquals(2, closeCalls.get());
        verify(fs, times(2)).close(file);
    }

    @Test
    public void closeHandleSkipsAfterTwoOperationNotExecuted() {
        AsyncFileSystem fs = mock(AsyncFileSystem.class);
        AsyncFile file = mock(AsyncFile.class);
        when(fs.close(any(AsyncFile.class))).thenThrow(new OperationNotExecutedException("still dirty"));

        AsyncFileSystemHelper.closeHandle(fs, file, "close retry skip");
        verify(fs, times(2)).close(file);
    }

    @Test
    public void closeHandleTimeoutDoesNotCancelFuture() throws Exception {
        AsyncFileSystem fs = mock(AsyncFileSystem.class);
        AsyncFile file = mock(AsyncFile.class);
        CompletableFuture<Void> closeFuture = new CompletableFuture<>();
        when(fs.close(any(AsyncFile.class))).thenReturn(closeFuture);

        Thread closer = new Thread(() -> {
            try {
                AsyncFileSystemHelper.runWithIoTimeout(50, () ->
                        AsyncFileSystemHelper.closeHandle(fs, file, "slow close"));
            } catch (IOException e) {
                Assert.fail("closeHandle must not throw: " + e);
            }
        });
        closer.start();
        closer.join(TimeUnit.SECONDS.toMillis(5));
        Assert.assertFalse(closer.isAlive());
        Assert.assertFalse("close future must not be cancelled on timeout", closeFuture.isCancelled());
        closeFuture.complete(null);
    }

    @Test
    public void closeReadHandleDoesNotAwaitIncompleteFuture() {
        AsyncFileSystem fs = mock(AsyncFileSystem.class);
        AsyncFile file = mock(AsyncFile.class);
        CompletableFuture<Void> closeFuture = new CompletableFuture<>();
        when(fs.close(any(AsyncFile.class))).thenReturn(closeFuture);

        long start = System.nanoTime();
        AsyncFileSystemHelper.closeReadHandle(fs, file, "read close no await");
        Assert.assertTrue("closeReadHandle must return without awaiting",
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) < 200);
        Assert.assertFalse(closeFuture.isCancelled());
        closeFuture.complete(null);
        verify(fs).close(file);
    }

    @Test
    public void closeReadHandleRetriesOnceOnOperationNotExecuted() {
        AsyncFileSystem fs = mock(AsyncFileSystem.class);
        AsyncFile file = mock(AsyncFile.class);
        AtomicInteger closeCalls = new AtomicInteger();
        when(fs.close(any(AsyncFile.class))).thenAnswer(invocation -> {
            if (closeCalls.getAndIncrement() == 0) {
                throw new OperationNotExecutedException("dirty");
            }
            return CompletableFuture.completedFuture(null);
        });

        AsyncFileSystemHelper.closeReadHandle(fs, file, "read close retry");
        Assert.assertEquals(2, closeCalls.get());
        verify(fs, times(2)).close(file);
    }

    @Test
    public void awaitRetriesOnceOnSyncOperationNotExecutedThenSucceeds() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        long result = AsyncFileSystemHelper.await(() -> {
            if (calls.getAndIncrement() == 0) {
                throw new OperationNotExecutedException("meta.v2.json");
            }
            return CompletableFuture.completedFuture(42L);
        }, "size meta");
        Assert.assertEquals(42L, result);
        Assert.assertEquals(2, calls.get());
    }

    @Test
    public void awaitThrowsAfterTwoSyncOperationNotExecuted() {
        AtomicInteger calls = new AtomicInteger();
        try {
            AsyncFileSystemHelper.await(() -> {
                calls.incrementAndGet();
                throw new OperationNotExecutedException("meta.v2.json");
            }, "write meta");
            Assert.fail("expected OperationNotExecutedException");
        } catch (OperationNotExecutedException e) {
            Assert.assertTrue(e.getMessage().contains("meta.v2.json"));
        } catch (IOException e) {
            Assert.fail("ONE must be rethrown as-is, not wrapped: " + e);
        }
        Assert.assertEquals(2, calls.get());
    }

    @Test
    public void writeAllBytesRetriesOnceOnOperationNotExecuted() throws Exception {
        AsyncFileSystem fs = mock(AsyncFileSystem.class);
        AsyncFile file = mock(AsyncFile.class);
        byte[] payload = new byte[]{1, 2, 3};
        AtomicInteger writes = new AtomicInteger();
        when(fs.write(any(AsyncFile.class), any(io.netty.buffer.ByteBuf.class))).thenAnswer(invocation -> {
            io.netty.buffer.ByteBuf buf = invocation.getArgument(1);
            if (writes.getAndIncrement() == 0) {
                buf.release();
                throw new OperationNotExecutedException("meta.v2.json");
            }
            long n = buf.readableBytes();
            buf.release();
            return CompletableFuture.completedFuture(n);
        });

        AsyncFileSystemHelper.writeAllBytes(fs, file, payload, "write meta");
        Assert.assertEquals(2, writes.get());
        verify(fs, times(2)).write(any(AsyncFile.class), any(io.netty.buffer.ByteBuf.class));
    }

    @Test
    public void writeAndAwaitSegmentRetriesOnceOnOperationNotExecuted() throws Exception {
        AsyncFileSystem fs = mock(AsyncFileSystem.class);
        AsyncSegmentFile segment = mock(AsyncSegmentFile.class);
        io.netty.buffer.ByteBuf chunk = io.netty.buffer.Unpooled.wrappedBuffer(new byte[]{9, 8, 7, 6});
        AtomicInteger writes = new AtomicInteger();
        when(fs.write(any(AsyncSegmentFile.class), any(io.netty.buffer.ByteBuf.class))).thenAnswer(invocation -> {
            io.netty.buffer.ByteBuf buf = invocation.getArgument(1);
            if (writes.getAndIncrement() == 0) {
                buf.release();
                throw new OperationNotExecutedException("cmd segment");
            }
            long n = buf.readableBytes();
            buf.release();
            return CompletableFuture.completedFuture(n);
        });

        long flushed = AsyncFileSystemHelper.writeAndAwait(fs, segment, chunk, 4, "write command segment");
        Assert.assertEquals(4L, flushed);
        Assert.assertEquals(2, writes.get());
        Assert.assertEquals("guard retain released after success", 0, chunk.refCnt());
    }

    @Test
    public void writeAndAwaitThrowsAfterTwoOperationNotExecuted() {
        AsyncFileSystem fs = mock(AsyncFileSystem.class);
        AsyncFile file = mock(AsyncFile.class);
        io.netty.buffer.ByteBuf data = io.netty.buffer.Unpooled.wrappedBuffer(new byte[]{1});
        when(fs.write(any(AsyncFile.class), any(io.netty.buffer.ByteBuf.class))).thenAnswer(invocation -> {
            io.netty.buffer.ByteBuf buf = invocation.getArgument(1);
            buf.release();
            throw new OperationNotExecutedException("rdb");
        });

        try {
            AsyncFileSystemHelper.writeAndAwait(fs, file, data, 1, "write rdb");
            Assert.fail("expected OperationNotExecutedException");
        } catch (OperationNotExecutedException e) {
            Assert.assertTrue(e.getMessage().contains("rdb"));
        } catch (IOException e) {
            Assert.fail("ONE must be rethrown as-is: " + e);
        }
        verify(fs, times(2)).write(any(AsyncFile.class), any(io.netty.buffer.ByteBuf.class));
        Assert.assertEquals(0, data.refCnt());
    }

    @Test
    public void writeAndAwaitRetriesOnceOnSyncIllegalStateThenSucceeds() throws Exception {
        AsyncFileSystem fs = mock(AsyncFileSystem.class);
        AsyncFile file = mock(AsyncFile.class);
        io.netty.buffer.ByteBuf data = io.netty.buffer.Unpooled.wrappedBuffer(new byte[]{1, 2});
        AtomicInteger writes = new AtomicInteger();
        when(fs.write(any(AsyncFile.class), any(io.netty.buffer.ByteBuf.class))).thenAnswer(invocation -> {
            io.netty.buffer.ByteBuf buf = invocation.getArgument(1);
            if (writes.getAndIncrement() == 0) {
                buf.release();
                throw new IllegalStateException("file cache is closed");
            }
            long n = buf.readableBytes();
            buf.release();
            return CompletableFuture.completedFuture(n);
        });

        long flushed = AsyncFileSystemHelper.writeAndAwait(fs, file, data, 2, "write after cache closed");
        Assert.assertEquals(2L, flushed);
        Assert.assertEquals(2, writes.get());
        Assert.assertEquals(0, data.refCnt());
    }

    @Test
    public void writeAndAwaitThrowsAfterTwoSyncIllegalState() {
        AsyncFileSystem fs = mock(AsyncFileSystem.class);
        AsyncFile file = mock(AsyncFile.class);
        io.netty.buffer.ByteBuf data = io.netty.buffer.Unpooled.wrappedBuffer(new byte[]{1});
        IllegalStateException boom = new IllegalStateException("file cache is closed");
        when(fs.write(any(AsyncFile.class), any(io.netty.buffer.ByteBuf.class))).thenAnswer(invocation -> {
            io.netty.buffer.ByteBuf buf = invocation.getArgument(1);
            buf.release();
            throw boom;
        });

        try {
            AsyncFileSystemHelper.writeAndAwait(fs, file, data, 1, "write still closed");
            Assert.fail("expected IllegalStateException");
        } catch (IllegalStateException e) {
            Assert.assertSame(boom, e);
        } catch (IOException e) {
            Assert.fail("sync Exception must be rethrown as-is: " + e);
        }
        verify(fs, times(2)).write(any(AsyncFile.class), any(io.netty.buffer.ByteBuf.class));
        Assert.assertEquals("terminal failure releases caller buf", 0, data.refCnt());
    }

    @Test
    public void writeAndAwaitDoesNotRetryWhenFutureCompletesExceptionally() {
        AsyncFileSystem fs = mock(AsyncFileSystem.class);
        AsyncFile file = mock(AsyncFile.class);
        io.netty.buffer.ByteBuf data = io.netty.buffer.Unpooled.wrappedBuffer(new byte[]{1});
        AtomicInteger writes = new AtomicInteger();
        when(fs.write(any(AsyncFile.class), any(io.netty.buffer.ByteBuf.class))).thenAnswer(invocation -> {
            writes.incrementAndGet();
            io.netty.buffer.ByteBuf buf = invocation.getArgument(1);
            buf.release();
            CompletableFuture<Long> failed = new CompletableFuture<>();
            failed.completeExceptionally(new IOException("disk"));
            return failed;
        });

        try {
            AsyncFileSystemHelper.writeAndAwait(fs, file, data, 1, "write future fail");
            Assert.fail("expected IOException");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().contains("disk") || e.getCause() instanceof IOException);
        }
        Assert.assertEquals("must not rewrite after future is returned", 1, writes.get());
        Assert.assertEquals(0, data.refCnt());
    }

    @Test
    public void writeAndAwaitDoesNotRetryOnTimeout() throws Exception {
        AsyncFileSystem fs = mock(AsyncFileSystem.class);
        AsyncFile file = mock(AsyncFile.class);
        io.netty.buffer.ByteBuf data = io.netty.buffer.Unpooled.wrappedBuffer(new byte[]{1});
        CompletableFuture<Long> hung = new CompletableFuture<>();
        when(fs.write(any(AsyncFile.class), any(io.netty.buffer.ByteBuf.class))).thenReturn(hung);

        try {
            AsyncFileSystemHelper.runWithIoTimeout(50, () ->
                    AsyncFileSystemHelper.writeAndAwait(fs, file, data, 1, "slow write"));
            Assert.fail("expected timeout");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().contains("timeout"));
        }
        verify(fs, times(1)).write(any(AsyncFile.class), any(io.netty.buffer.ByteBuf.class));
        Assert.assertTrue("timeout still cancels write future", hung.isCancelled());
        Assert.assertEquals("guard retain dropped; FS still holds in-flight buf", 1, data.refCnt());
        data.release();
    }

    @Test
    public void writeAllBytesRetriesOnceOnSyncIllegalStateThenSucceeds() throws Exception {
        AsyncFileSystem fs = mock(AsyncFileSystem.class);
        AsyncFile file = mock(AsyncFile.class);
        byte[] payload = new byte[]{1, 2, 3};
        AtomicInteger writes = new AtomicInteger();
        when(fs.write(any(AsyncFile.class), any(io.netty.buffer.ByteBuf.class))).thenAnswer(invocation -> {
            io.netty.buffer.ByteBuf buf = invocation.getArgument(1);
            if (writes.getAndIncrement() == 0) {
                buf.release();
                throw new IllegalStateException("file cache is closed");
            }
            long n = buf.readableBytes();
            buf.release();
            return CompletableFuture.completedFuture(n);
        });

        AsyncFileSystemHelper.writeAllBytes(fs, file, payload, "write meta closed");
        Assert.assertEquals(2, writes.get());
        verify(fs, times(2)).write(any(AsyncFile.class), any(io.netty.buffer.ByteBuf.class));
    }

    @Test
    public void writeAllBytesDoesNotRetryWhenFutureCompletesExceptionally() {
        AsyncFileSystem fs = mock(AsyncFileSystem.class);
        AsyncFile file = mock(AsyncFile.class);
        byte[] payload = new byte[]{1, 2, 3};
        AtomicInteger writes = new AtomicInteger();
        when(fs.write(any(AsyncFile.class), any(io.netty.buffer.ByteBuf.class))).thenAnswer(invocation -> {
            writes.incrementAndGet();
            io.netty.buffer.ByteBuf buf = invocation.getArgument(1);
            buf.release();
            CompletableFuture<Long> failed = new CompletableFuture<>();
            failed.completeExceptionally(new IOException("disk"));
            return failed;
        });

        try {
            AsyncFileSystemHelper.writeAllBytes(fs, file, payload, "write meta future fail");
            Assert.fail("expected IOException");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().contains("disk") || e.getCause() instanceof IOException);
        }
        Assert.assertEquals(1, writes.get());
    }

    @Test
    public void classifyFailureMapsKnownAndUnknownCases() {
        Assert.assertEquals(AsyncFileSystemHelper.CASE_NOT_EXECUTED,
                AsyncFileSystemHelper.classifyFailure(new OperationNotExecutedException("x")));
        Assert.assertEquals(AsyncFileSystemHelper.CASE_MEMORY_RESERVE_FAIL,
                AsyncFileSystemHelper.classifyFailure(new CacheMemoryReserveException(8, 100, 99)));
        Assert.assertEquals(AsyncFileSystemHelper.CASE_UNKNOWN_FAIL,
                AsyncFileSystemHelper.classifyFailure(new IllegalStateException("file cache is closed")));
        Assert.assertEquals(AsyncFileSystemHelper.CASE_UNKNOWN_FAIL,
                AsyncFileSystemHelper.classifyFailure(new IOException("disk")));
    }

    @Test
    public void writeAndAwaitRetriesOnceOnCacheMemoryReserveThenSucceeds() throws Exception {
        AsyncFileSystem fs = mock(AsyncFileSystem.class);
        AsyncFile file = mock(AsyncFile.class);
        io.netty.buffer.ByteBuf data = io.netty.buffer.Unpooled.wrappedBuffer(new byte[]{1, 2});
        AtomicInteger writes = new AtomicInteger();
        when(fs.write(any(AsyncFile.class), any(io.netty.buffer.ByteBuf.class))).thenAnswer(invocation -> {
            io.netty.buffer.ByteBuf buf = invocation.getArgument(1);
            if (writes.getAndIncrement() == 0) {
                buf.release();
                throw new CacheMemoryReserveException(2, 100, 99);
            }
            long n = buf.readableBytes();
            buf.release();
            return CompletableFuture.completedFuture(n);
        });

        long flushed = AsyncFileSystemHelper.writeAndAwait(fs, file, data, 2, "write memory reserve");
        Assert.assertEquals(2L, flushed);
        Assert.assertEquals(2, writes.get());
        Assert.assertEquals(0, data.refCnt());
    }
}
