package com.ctrip.xpipe.redis.keeper.storage;

import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import io.netty.buffer.Unpooled;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * keeper-tfs-m6 P5 (spec §4): opening a file whose handle is still pending close is reported, not
 * refused. The FS keeps its own {@code writer already open} exclusion.
 */
public class AsyncFileSystemHelperReopenReportTest extends AbstractRedisKeeperTest {

    private static final ReplId REPL_ID = ReplId.from(11L);
    private static final ReplId PENDING_REPL_ID = ReplId.from(12L);
    private static final long WINDOW_MILLIS = 60_000L;
    private static final String TAG = "[awaitOpen][reopen-pending-close]";
    private static final String SEG_PREFIX = "seg";
    private static final String IDX_PREFIX = "idx";
    private static final List<String> INDEX_PREFIXES = Collections.singletonList(IDX_PREFIX);

    private final AtomicLong clock = new AtomicLong(1_000_000L);
    private DeferredHandleCloserTest.ManualScheduler scheduler;
    private AsyncFileSystem fs;
    private DeferredHandleCloser closer;
    private AsyncFileSystem realFs;
    private DeferredHandleCloser realCloser;

    @Before
    public void setUpCloser() {
        scheduler = new DeferredHandleCloserTest.ManualScheduler(clock);
        fs = mock(AsyncFileSystem.class);
        when(fs.open(anyString(), any(AbstractStorageFile.OpenMode.class),
                any(AbstractStorageFile.ReplaceMode.class), anyBoolean(), nullable(String.class)))
                .thenAnswer(inv -> CompletableFuture.completedFuture(
                        file(inv.getArgument(0), inv.getArgument(1))));
        when(fs.open(anyString(), anyString(), anyList(), anyBoolean(), nullable(String.class)))
                .thenAnswer(inv -> CompletableFuture.completedFuture(
                        segment(inv.getArgument(0), inv.getArgument(1), inv.getArgument(3))));
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

    @Test
    public void testReopenIsAllowedAndReportedWithPendingHandleDetails() throws Exception {
        String path = "/data/repl-12/meta.v2.json";
        long deferredAt = clock.get();
        defer(file(path, AbstractStorageFile.OpenMode.WRITE));

        try (LogCapture logs = new LogCapture(AsyncFileSystemHelper.class)) {
            AsyncFile reopened = openFile(path, AbstractStorageFile.OpenMode.READ);
            assertEquals(StorageUtil.asyncFileKey(path), reopened.getKey());

            List<LogEvent> events = logs.withTag(TAG);
            assertEquals(1, events.size());
            assertEquals(Level.WARN, events.get(0).getLevel());
            String message = LogCapture.message(events.get(0));
            assertTrue(message, message.contains("repl_id=" + PENDING_REPL_ID));
            assertTrue(message, message.contains("fileKey=f|" + path));
            assertTrue(message, message.contains("write=true"));
            assertTrue(message, message.contains("mode=FLUSH"));
            assertTrue(message, message.contains("open: open " + path));
        }
        assertEquals("the pending handle is untouched", deferredAt,
                closer.findPending(StorageUtil.asyncFileKey(path)).getFirstFailureTimeMillis());
    }

    @Test
    public void testSegmentReopenIsReportedOnTheSegmentKey() throws Exception {
        String dir = "/data/repl-12";
        defer(segment(dir, SEG_PREFIX, false));

        try (LogCapture logs = new LogCapture(AsyncFileSystemHelper.class)) {
            assertNotNull(openSegment(dir, SEG_PREFIX, true));
            assertEquals(1, logs.withTag(TAG).size());
            assertTrue(LogCapture.message(logs.withTag(TAG).get(0))
                    .contains("fileKey=s|" + dir + "|" + SEG_PREFIX));

            // Another prefix in the same directory is another key.
            assertNotNull(openSegment(dir, "rdb", true));
            assertEquals(1, logs.withTag(TAG).size());
        }
    }

    @Test
    public void testEveryReopenIsLoggedWithTheCurrentAge() throws Exception {
        String path = "/data/repl-12/cmd";
        defer(file(path, AbstractStorageFile.OpenMode.READ));

        try (LogCapture logs = new LogCapture(AsyncFileSystemHelper.class)) {
            for (int i = 0; i < 3; i++) {
                clock.addAndGet(20_000L);
                openFile(path, AbstractStorageFile.OpenMode.READ);
            }
            List<LogEvent> events = logs.withTag(TAG);
            assertEquals("not rate limited: the reopen loop is slow enough", 3, events.size());
            assertTrue(LogCapture.message(events.get(0)).contains("age=20000ms"));
            assertTrue(LogCapture.message(events.get(2)).contains("age=60000ms"));
        }
    }

    @Test
    public void testNoPendingHandleAndNoCloserAreSilent() throws Exception {
        AsyncFileSystem bare = mock(AsyncFileSystem.class);
        when(bare.open(anyString(), any(AbstractStorageFile.OpenMode.class),
                any(AbstractStorageFile.ReplaceMode.class), anyBoolean(), nullable(String.class)))
                .thenAnswer(inv -> CompletableFuture.completedFuture(
                        file(inv.getArgument(0), inv.getArgument(1))));
        assertNull(DeferredHandleCloser.of(bare));
        defer(file("/data/repl-12/other", AbstractStorageFile.OpenMode.WRITE));

        try (LogCapture logs = new LogCapture(AsyncFileSystemHelper.class)) {
            openFile("/data/repl-12/fresh", AbstractStorageFile.OpenMode.WRITE);
            assertNotNull(AsyncFileSystemHelper.awaitOpen(bare,
                    () -> bare.open("/data/repl-12/other", AbstractStorageFile.OpenMode.READ,
                            AbstractStorageFile.ReplaceMode.NORMAL, true, null),
                    "open without closer"));
            assertTrue(logs.withTag(TAG).isEmpty());
        }
    }

    // ---- real FS: the reported key is the key of the opened handle ----

    @Test
    public void testPendingWriteHandleStillBlocksWriteOpenAtFsLevelOnRealFs() throws Exception {
        newRealFs();
        String path = new File(getTestFileDir(), "key-check.bin").getAbsolutePath();
        AsyncFile writer = AsyncFileSystemHelper.awaitOpen(realFs,
                () -> realFs.open(path, AbstractStorageFile.OpenMode.WRITE,
                        AbstractStorageFile.ReplaceMode.NORMAL, true, REPL_ID.toString()),
                "open for write");
        assertEquals(StorageUtil.asyncFileKey(path), writer.getKey());
        realCloser.defer(writer, REPL_ID, "deferred write handle", new IOException("injected"));

        try (LogCapture logs = new LogCapture(AsyncFileSystemHelper.class)) {
            // Read open goes through and is reported under the key of the opened handle.
            AsyncFile reader = AsyncFileSystemHelper.awaitOpen(realFs,
                    () -> realFs.open(path, AbstractStorageFile.OpenMode.READ,
                            AbstractStorageFile.ReplaceMode.NORMAL, true, REPL_ID.toString()),
                    "reopen for read");
            assertEquals(1, logs.withTag(TAG).size());
            assertTrue(LogCapture.message(logs.withTag(TAG).get(0)).contains("fileKey=f|" + path));
            AsyncFileSystemHelper.closeReadHandle(realFs, reader, REPL_ID, "close reader");

            // Write open is still refused by the FS itself; m6 adds no check.
            try {
                AsyncFileSystemHelper.awaitOpen(realFs,
                        () -> realFs.open(path, AbstractStorageFile.OpenMode.WRITE,
                                AbstractStorageFile.ReplaceMode.NORMAL, true, REPL_ID.toString()),
                        "reopen for write");
                fail("expected the FS write exclusion to refuse the reopen");
            } catch (IllegalStateException expected) {
                assertTrue(expected.getMessage(), expected.getMessage().contains("writer already open"));
            }
        }

        // Once the deferred close succeeds the write open works again.
        scheduler.runNext();
        assertEquals(0, realCloser.count());
        assertTrue(writer.closed);
        AsyncFile again = AsyncFileSystemHelper.awaitOpen(realFs,
                () -> realFs.open(path, AbstractStorageFile.OpenMode.WRITE,
                        AbstractStorageFile.ReplaceMode.NORMAL, true, REPL_ID.toString()),
                "reopen for write");
        assertTrue(AsyncFileSystemHelper.closeHandle(realFs, again, REPL_ID, "close reopened"));
    }

    @Test
    public void testIndexHandleIsReportedUnderTheSegmentKeyOnRealFs() throws Exception {
        newRealFs();
        String dir = new File(getTestFileDir(), "segkey").getAbsolutePath();
        Files.createDirectories(Paths.get(dir));
        String segmentKey = StorageUtil.segmentKey(dir, SEG_PREFIX);

        AsyncSegmentFile writer = AsyncFileSystemHelper.awaitOpen(realFs,
                () -> realFs.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, REPL_ID.toString()),
                "open segment for write");
        assertEquals(segmentKey, writer.getKey());
        AsyncFileSystemHelper.writeAndAwait(realFs, writer, Unpooled.wrappedBuffer(new byte[]{1, 2, 3}), 3,
                "write segment");
        AsyncFile index = AsyncFileSystemHelper.await(() -> realFs.getCurrentIndexFiles(writer, INDEX_PREFIXES),
                "get index files").getValue().get(IDX_PREFIX);
        assertNotNull(index);
        assertEquals("index files only open through their segment", segmentKey, index.getKey());

        realCloser.defer(index, REPL_ID, "deferred index handle", new IOException("injected"));
        try (LogCapture logs = new LogCapture(AsyncFileSystemHelper.class)) {
            AsyncSegmentFile reader = AsyncFileSystemHelper.awaitOpen(realFs,
                    () -> realFs.open(dir, SEG_PREFIX, INDEX_PREFIXES, false, REPL_ID.toString()),
                    "reopen segment for read");
            assertEquals(1, logs.withTag(TAG).size());
            assertTrue(LogCapture.message(logs.withTag(TAG).get(0)).contains("fileKey=s|" + dir + "|" + SEG_PREFIX));
            AsyncFileSystemHelper.closeReadHandle(realFs, reader, REPL_ID, "close segment reader");
        }
        AsyncFileSystemHelper.closeHandle(realFs, writer, REPL_ID, "close segment");
    }

    // ---- helpers ----

    private void newRealFs() {
        realFs = createTestAsyncFileSystem();
        realCloser = new DeferredHandleCloser(realFs, () -> WINDOW_MILLIS, clock::get, scheduler.executor);
        DeferredHandleCloser.register(realFs, realCloser);
    }

    private void defer(AbstractStorageFile file) {
        assertTrue(closer.defer(file, PENDING_REPL_ID, "close " + file.getKey(),
                new OperationNotExecutedException("in-flight")));
    }

    private AsyncFile openFile(String path, AbstractStorageFile.OpenMode openMode) throws IOException {
        return AsyncFileSystemHelper.awaitOpen(fs,
                () -> fs.open(path, openMode, AbstractStorageFile.ReplaceMode.NORMAL, true, REPL_ID.toString()),
                "open " + path + " as " + openMode);
    }

    private AsyncSegmentFile openSegment(String dir, String prefix, boolean write) throws IOException {
        return AsyncFileSystemHelper.awaitOpen(fs,
                () -> fs.open(dir, prefix, INDEX_PREFIXES, write, REPL_ID.toString()),
                "open segment " + dir + " write=" + write);
    }

    private static AsyncFile file(String path, AbstractStorageFile.OpenMode mode) {
        String key = StorageUtil.asyncFileKey(path);
        return new AsyncFile(path, AbstractStorageFile.ReplaceMode.NORMAL, mode, key, key, false);
    }

    private static AsyncSegmentFile segment(String dir, String prefix, boolean write) {
        String key = StorageUtil.segmentKey(dir, prefix);
        return new AsyncSegmentFile(dir, prefix, Collections.emptyList(), key, key, write);
    }
}
