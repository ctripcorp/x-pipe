package com.ctrip.xpipe.redis.keeper.storage;

import com.ctrip.xpipe.redis.core.protocal.protocal.LenEofType;
import com.ctrip.xpipe.redis.core.store.RdbStore;
import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.store.meta.DefaultMetaStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * keeper-tfs-m6 P2 (spec §2.2, §2.4): Helper close failures go to the FS's DeferredHandleCloser.
 */
public class AsyncFileSystemHelperDeferredCloseTest extends AbstractRedisKeeperTest {

    private static final ReplId REPL_ID = ReplId.from(7L);
    private static final long WINDOW_MILLIS = 60_000L;

    private final AtomicLong clock = new AtomicLong(1_000_000L);
    private DeferredHandleCloserTest.ManualScheduler scheduler;
    private AsyncFileSystem fs;
    private DeferredHandleCloser closer;
    private DeferredHandleCloser realCloser;
    private AsyncFileSystem realFs;

    @Before
    public void setUpCloser() {
        scheduler = new DeferredHandleCloserTest.ManualScheduler(clock);
        fs = mock(AsyncFileSystem.class);
        // Closer retries use the two-arg close; the Helper uses the one-arg close.
        when(fs.close(any(AsyncFile.class), anyBoolean())).thenReturn(CompletableFuture.completedFuture(null));
        when(fs.close(any(AsyncSegmentFile.class), anyBoolean())).thenReturn(CompletableFuture.completedFuture(null));
        closer = new DeferredHandleCloser(fs, () -> WINDOW_MILLIS, clock::get, scheduler.executor);
        DeferredHandleCloser.register(fs, closer);
    }

    @After
    public void tearDownCloser() {
        closer.shutdown(0);
        if (realCloser != null) {
            realCloser.shutdown(0);
        }
        if (realFs != null) {
            realFs.shutdown();
        }
    }

    private static AsyncFile file(String path, AbstractStorageFile.OpenMode mode) {
        String key = StorageUtil.asyncFileKey(path);
        return new AsyncFile(path, AbstractStorageFile.ReplaceMode.NORMAL, mode, key, key, false);
    }

    // ---- second ONE ----

    @Test
    public void testWriteHandleIsDeferredAfterSecondNotExecuted() {
        AsyncFile writer = file("/data/repl-7/meta.v2.json", AbstractStorageFile.OpenMode.WRITE);
        when(fs.close(any(AsyncFile.class))).thenThrow(new OperationNotExecutedException("in-flight"));

        assertFalse(AsyncFileSystemHelper.closeHandle(fs, writer, REPL_ID, "close meta"));
        verify(fs, times(2)).close(writer);
        DeferredHandle handle = closer.findPending(writer.getKey());
        assertSame(writer, handle.file);
        assertEquals(REPL_ID, handle.getReplId());
        assertTrue(handle.isWrite());
        assertEquals("close meta", handle.getOperation());
        assertTrue(handle.getLastError() instanceof OperationNotExecutedException);

        scheduler.runNext();
        verify(fs).close(writer, false);
        assertEquals(0, closer.count());
    }

    @Test
    public void testReadHandleIsDeferredAfterSecondNotExecuted() {
        AsyncFile reader = file("/data/repl-7/rdb_1", AbstractStorageFile.OpenMode.READ);
        when(fs.close(any(AsyncFile.class))).thenThrow(new OperationNotExecutedException("read in-flight"));

        AsyncFileSystemHelper.closeReadHandle(fs, reader, REPL_ID, "close rdb read");
        verify(fs, times(2)).close(reader);
        DeferredHandle handle = closer.findPending(reader.getKey());
        assertSame(reader, handle.file);
        assertFalse(handle.isWrite());
        assertEquals(REPL_ID, handle.getReplId());

        scheduler.runNext();
        verify(fs).close(reader, true);
        assertEquals(0, closer.count());
    }

    @Test
    public void testSegmentIsDeferredAfterSecondNotExecuted() {
        String key = StorageUtil.segmentKey("/data/repl-7", "cmd_");
        AsyncSegmentFile segment = new AsyncSegmentFile("/data/repl-7", "cmd_", Collections.emptyList(),
                key, key, true);
        when(fs.close(any(AsyncSegmentFile.class))).thenThrow(new OperationNotExecutedException("index dirty"));

        assertFalse(AsyncFileSystemHelper.closeHandle(fs, segment, REPL_ID, "close command segment"));
        verify(fs, times(2)).close(segment);
        assertSame(segment, closer.findPending(key).file);

        scheduler.runNext();
        verify(fs).close(segment, false);
        assertEquals(0, closer.count());
    }

    // ---- other failures ----

    @Test
    public void testNonNotExecutedFailuresAreDeferredWithoutImmediateRetry() {
        AsyncFile syncFail = file("/data/repl-7/a", AbstractStorageFile.OpenMode.WRITE);
        AsyncFile futureFail = file("/data/repl-7/b", AbstractStorageFile.OpenMode.WRITE);
        AsyncFile readFutureFail = file("/data/repl-7/c", AbstractStorageFile.OpenMode.READ);
        RuntimeException enospc = StorageUtil.wrapIOException(new IOException("No space left on device"));
        when(fs.close(syncFail)).thenThrow(enospc);
        when(fs.close(futureFail)).thenReturn(CompletableFuture.failedFuture(new IOException("EIO")));
        when(fs.close(readFutureFail)).thenReturn(CompletableFuture.failedFuture(new IOException("EIO read")));

        assertFalse(AsyncFileSystemHelper.closeHandle(fs, syncFail, REPL_ID, "close a"));
        assertFalse(AsyncFileSystemHelper.closeHandle(fs, futureFail, REPL_ID, "close b"));
        AsyncFileSystemHelper.closeReadHandle(fs, readFutureFail, REPL_ID, "close c");

        verify(fs, times(1)).close(syncFail);
        verify(fs, times(1)).close(futureFail);
        verify(fs, times(1)).close(readFutureFail);
        assertSame(enospc, closer.findPending(syncFail.getKey()).getLastError());
        assertTrue(closer.findPending(futureFail.getKey()).getLastError() instanceof IOException);
        assertTrue(closer.isPending(readFutureFail));
        assertEquals(3, closer.count());
        assertEquals(2, closer.writeCount());
    }

    @Test
    public void testLateFailureAfterTimeoutIsDeferredOnceCloseFinished() throws Exception {
        AsyncFile writer = file("/data/repl-7/slow", AbstractStorageFile.OpenMode.WRITE);
        CompletableFuture<Void> closeFuture = new CompletableFuture<>();
        when(fs.close(writer)).thenReturn(closeFuture);
        AtomicBoolean closed = new AtomicBoolean(true);

        AsyncFileSystemHelper.runWithIoTimeout(50, () ->
                closed.set(AsyncFileSystemHelper.closeHandle(fs, writer, REPL_ID, "slow close")));
        assertFalse("timed out close is not reported as closed", closed.get());
        assertFalse("close still running, the closer must not own it yet", closer.isPending(writer));
        assertFalse(closeFuture.isCancelled());

        closeFuture.completeExceptionally(new IOException("late EIO"));
        assertTrue(closer.isPending(writer));
    }

    @Test
    public void testProgrammingErrorIsNotDeferred() {
        AsyncFile notAllowed = file("/data/repl-7/x", AbstractStorageFile.OpenMode.WRITE);
        when(fs.close(notAllowed)).thenThrow(new IllegalStateException("close is not allowed for: x"));

        assertFalse(AsyncFileSystemHelper.closeHandle(fs, notAllowed, REPL_ID, "close x"));
        verify(fs, times(1)).close(notAllowed);
        assertEquals(0, closer.count());
        assertTrue(scheduler.tasks.isEmpty());
    }

    // ---- ownership ----

    @Test
    public void testPendingHandleIsNotClosedAgain() {
        AsyncFile writer = file("/data/repl-7/pending", AbstractStorageFile.OpenMode.WRITE);
        when(fs.close(any(AsyncFile.class))).thenThrow(new OperationNotExecutedException("in-flight"));
        assertFalse(AsyncFileSystemHelper.closeHandle(fs, writer, REPL_ID, "close pending"));
        clearInvocations(fs);

        assertFalse(AsyncFileSystemHelper.closeHandle(fs, writer, REPL_ID, "close pending again"));
        AsyncFileSystemHelper.closeReadHandle(fs, writer, REPL_ID, "close pending as read");
        verify(fs, never()).close(any(AsyncFile.class));
        verify(fs, never()).close(any(AsyncFile.class), anyBoolean());
        assertEquals(1, closer.count());
        assertEquals("not scheduled twice", 1, scheduler.tasks.size());
    }

    // ---- compatibility ----

    @Test
    public void testFsWithoutCloserKeepsSkipAndSuccessReturnsTrue() {
        AsyncFileSystem bare = mock(AsyncFileSystem.class);
        AsyncFile failing = file("/data/repl-7/bare", AbstractStorageFile.OpenMode.WRITE);
        AsyncFile ok = file("/data/repl-7/ok", AbstractStorageFile.OpenMode.WRITE);
        when(bare.close(failing)).thenThrow(new OperationNotExecutedException("dirty"));
        when(bare.close(ok)).thenReturn(CompletableFuture.completedFuture(null));
        assertNull(DeferredHandleCloser.of(bare));

        assertFalse(AsyncFileSystemHelper.closeHandle(bare, failing, REPL_ID, "close bare"));
        verify(bare, times(2)).close(failing);
        assertTrue(AsyncFileSystemHelper.closeHandle(bare, ok, REPL_ID, "close ok"));
        assertTrue("nothing to close", AsyncFileSystemHelper.closeHandle(bare, null, REPL_ID, "close null"));
        assertEquals(0, closer.count());
    }

    // ---- real store on TailCache: the closer outlives the keeper ----

    @Test
    public void testDeferredMetaCloseIsRetriedAfterStoreGoneAndReleasesMemory() throws Exception {
        realFs = spy(createTestAsyncFileSystem());
        TailCacheFileSystem tcf = (TailCacheFileSystem) realFs;
        realCloser = new DeferredHandleCloser(realFs, () -> WINDOW_MILLIS, clock::get, scheduler.executor);
        DeferredHandleCloser.register(realFs, realCloser);

        DefaultMetaStore store = new DefaultMetaStore(new File(getTestFileDir()), randomKeeperRunid(), realFs,
                REPL_ID);
        store.initialize();
        store.rdbConfirmPsync("000000000000000000000000000000000000000B", 100, 0, "rdb_x",
                RdbStore.Type.NORMAL, new LenEofType(10), "cmd_m6_");
        assertTrue("meta is cached", tcf.getGlobalCommittedBytes() > 0);

        // Helper close and its immediate retry fail, as with a wedged in-flight write.
        doThrow(new OperationNotExecutedException("in-flight"))
                .doThrow(new OperationNotExecutedException("in-flight"))
                .doCallRealMethod()
                .when(realFs).close(any(AsyncFile.class), anyBoolean());
        assertFalse("deferred meta write handle is reported", store.close(false));

        // Keeper removed: nothing holds the store any more; only the container-level closer has the handle.
        store = null;
        assertEquals(1, realCloser.count());
        DeferredHandle handle = realCloser.snapshot().get(0);
        assertEquals(REPL_ID, handle.getReplId());
        assertTrue(handle.isWrite());
        assertFalse(handle.file.closed);
        assertTrue("memory pinned while pending", tcf.getGlobalCommittedBytes() > 0);

        scheduler.runNext();
        assertEquals(0, realCloser.count());
        assertTrue(handle.file.closed);
        assertEquals("tracker committed returned", 0, tcf.getGlobalCommittedBytes());
    }
}
