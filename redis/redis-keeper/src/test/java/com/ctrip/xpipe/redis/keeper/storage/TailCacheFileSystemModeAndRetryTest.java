package com.ctrip.xpipe.redis.keeper.storage;

import com.ctrip.xpipe.tuple.Pair;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.Assert.*;

/**
 * Mode-transition and retry/idempotency contracts that need deterministic fault injection.
 * Production code is deliberately treated as a black box except for package-visible state that is
 * part of the storage implementation's recovery protocol.
 */
public class TailCacheFileSystemModeAndRetryTest {

    private static final long CHUNK_SIZE = 64;
    private static final String SEG_PREFIX = "seg";
    private static final String IDX_PREFIX = "idx";
    private static final List<String> INDEX_PREFIXES = Collections.singletonList(IDX_PREFIX);

    private Path tempDir;
    private ExecutorService ioExecutor;
    private InstrumentedDelegate delegate;
    private TailCacheFileSystem fs;
    private final List<TailCacheFileSystem> extraFileSystems = new ArrayList<>();

    @Before
    public void setUp() throws Exception {
        tempDir = Files.createTempDirectory("tailcache-mode-retry-");
        ioExecutor = Executors.newCachedThreadPool();
        delegate = new InstrumentedDelegate(ioExecutor);
        fs = new TailCacheFileSystem(delegate, baseConfig(), ioExecutor);
    }

    @After
    public void tearDown() {
        delegate.releaseFault();
        for (TailCacheFileSystem extra : extraFileSystems) {
            extra.shutdown();
        }
        fs.shutdown();
        deleteRecursively(tempDir.toFile());
    }

    private TailCacheFileSystemConfig baseConfig() {
        return new TailCacheFileSystemConfig()
                .setPerFileCacheLimits(64 * 1024, 1, CHUNK_SIZE)
                .setMaxCacheSizeBytes(1024 * 1024)
                .setWriteBatchBytes(128)
                .setIoWaitTimeoutMs(5000)
                .setRestoreWaitTimeoutMs(5000)
                .setExpectedMinRetentionMs(0)
                .setEvictScanIntervalMs(60_000)
                .setWatermarkRatios(0.5, 0.8)
                .setMaxEvictRatioPerWrite(0.5);
    }

    private TailCacheFileSystemConfig noFsConfig() {
        return baseConfig().setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.NO_FS);
    }

    private TailCacheFileSystem newFileSystem(TailCacheFileSystemConfig config) {
        TailCacheFileSystem created = new TailCacheFileSystem(delegate, config, ioExecutor);
        extraFileSystems.add(created);
        return created;
    }

    private String path(String name) {
        return tempDir.resolve(name).toString();
    }

    private ByteBuf buf(byte... bytes) {
        return Unpooled.wrappedBuffer(bytes);
    }

    private ByteBuf buf(byte[] bytes, boolean ignored) {
        return Unpooled.wrappedBuffer(bytes);
    }

    private byte[] bytes(int... values) {
        byte[] result = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            result[i] = (byte) values[i];
        }
        return result;
    }

    private byte[] read(ByteBuf data) {
        try {
            byte[] result = new byte[data.readableBytes()];
            data.readBytes(result);
            return result;
        } finally {
            data.release();
        }
    }

    private void write(TailCacheFileSystem target, AsyncFile file, byte[] data) throws Exception {
        assertEquals(data.length, (long) target.write(file, buf(data, true)).get(5, TimeUnit.SECONDS));
    }

    private void write(TailCacheFileSystem target, AsyncSegmentFile file, byte[] data) throws Exception {
        assertEquals(data.length, (long) target.write(file, buf(data, true)).get(5, TimeUnit.SECONDS));
    }

    private AsyncFile index(TailCacheFileSystem target, AsyncSegmentFile file) throws Exception {
        Pair<Long, Map<String, AsyncFile>> current =
                target.getCurrentIndexFiles(file, INDEX_PREFIXES).get(5, TimeUnit.SECONDS);
        AsyncFile result = current.getValue().get(IDX_PREFIX);
        assertNotNull(result);
        return result;
    }

    private void expectFailure(CheckedRunnable operation) throws Exception {
        try {
            operation.run();
            fail("expected operation to fail");
        } catch (ExecutionException expected) {
            assertNotNull(expected.getCause());
        } catch (OperationNotExecutedException | StorageIOException | IllegalStateException expected) {
            // Synchronous barriers and flushes intentionally surface their failure directly.
        }
    }

    private void waitUntil(String message, BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertTrue(message, condition.getAsBoolean());
    }

    private AsyncSegmentFile buildThreeSegments(TailCacheFileSystem target, String dir) throws Exception {
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile writer = target.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get(5, TimeUnit.SECONDS);
        write(target, writer, bytes(1, 2, 3, 4));
        target.roll(writer).get(5, TimeUnit.SECONDS);
        write(target, writer, bytes(5, 6, 7, 8));
        target.roll(writer).get(5, TimeUnit.SECONDS);
        write(target, writer, bytes(9, 10, 11, 12));
        target.fsync(writer).get(5, TimeUnit.SECONDS);
        assertEquals(Arrays.asList(0L, 4L, 8L), target.list(writer));
        return writer;
    }

    private static void deleteRecursively(File file) {
        if (file == null || !file.exists()) return;
        File[] children = file.listFiles();
        if (children != null) {
            for (File child : children) deleteRecursively(child);
        }
        file.delete();
    }

    // -------------------------------------------------------------------------
    // Mode switching (6)
    // -------------------------------------------------------------------------

    @Test
    public void testAsyncNoFsAsyncDrainsPriorInFlightWriteWithoutLossOrDuplication() throws Exception {
        String p = path("switch-inflight");
        AsyncFile writer = fs.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        byte[] first = new byte[200];
        Arrays.fill(first, (byte) 1);

        delegate.hang(Op.FILE_WRITE);
        write(fs, writer, first);
        delegate.awaitEntered();
        fs.setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.NO_FS);
        write(fs, writer, bytes(2, 3, 4));
        assertEquals(203, writer.getCacheEntry().cacheEndOffset);

        fs.setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.ASYNC);
        CompletableFuture<Void> drain = CompletableFuture.runAsync(() -> {
            try {
                fs.fsync(writer).get(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        Thread.sleep(50);
        assertFalse("fsync must wait for the canonical in-flight write", drain.isDone());
        delegate.releaseFault();
        drain.get(5, TimeUnit.SECONDS);

        byte[] expected = new byte[203];
        Arrays.fill(expected, 0, 200, (byte) 1);
        expected[200] = 2;
        expected[201] = 3;
        expected[202] = 4;
        assertArrayEquals(expected, Files.readAllBytes(Paths.get(p)));
        assertEquals(203, writer.getCacheEntry().writtenToFsOffset);
        assertEquals(2, delegate.count(Op.FILE_WRITE));
        fs.close(writer).get();
    }

    @Test
    public void testNoFsCloseAndReopenIsNotCorruptedByLateInFlightWrite() throws Exception {
        String p = path("late-write");
        AsyncFile oldWriter = fs.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        byte[] stale = new byte[200];
        Arrays.fill(stale, (byte) 7);
        delegate.hang(Op.FILE_WRITE);
        write(fs, oldWriter, stale);
        delegate.awaitEntered();

        fs.setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.NO_FS);
        fs.close(oldWriter).get(5, TimeUnit.SECONDS);
        assertTrue(oldWriter.closed);
        AsyncFile replacement = fs.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        write(fs, replacement, bytes(1, 2, 3));
        assertArrayEquals(bytes(1, 2, 3), read(fs.read(replacement, 3, 0).get()));

        fs.setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.ASYNC);
        CompletableFuture<Void> flush = CompletableFuture.runAsync(() -> {
            try {
                fs.fsync(replacement).get(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        delegate.releaseFault();
        flush.get(5, TimeUnit.SECONDS);

        assertArrayEquals("late work owned by the closed handle must not overwrite the replacement",
                bytes(1, 2, 3), Files.readAllBytes(Paths.get(p)));
        assertEquals(3, replacement.getCacheEntry().writtenToFsOffset);
        fs.close(replacement).get();
    }

    @Test
    public void testAsyncNoFsAsyncRestoresTruncatedRegularFileAndAppendedSuffix() throws Exception {
        String p = path("regular-restore");
        AsyncFile writer = fs.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        write(fs, writer, bytes(1, 2, 3, 4, 5, 6, 7, 8));
        fs.fsync(writer).get();

        fs.setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.NO_FS);
        fs.truncate(writer, 4).get();
        write(fs, writer, bytes(9, 10));
        FileCacheEntry entry = writer.getCacheEntry();
        assertTrue(entry.fsInconsistent);
        assertEquals(6, entry.cacheEndOffset);
        assertArrayEquals(bytes(1, 2, 3, 4, 5, 6, 7, 8), Files.readAllBytes(Paths.get(p)));

        fs.setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.ASYNC);
        fs.fsync(writer).get();
        assertFalse(entry.fsInconsistent);
        assertEquals(6, entry.writtenToFsOffset);
        assertArrayEquals(bytes(1, 2, 3, 4, 9, 10), Files.readAllBytes(Paths.get(p)));
        fs.close(writer).get();
    }

    @Test
    public void testAsyncNoFsDeleteAllSegmentsCleansOrphansOnReturnToAsync() throws Exception {
        String dir = path("delete-all-restore");
        AsyncSegmentFile writer = buildThreeSegments(fs, dir);
        assertTrue(Files.exists(Paths.get(dir, SEG_PREFIX + "0")));

        fs.setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.NO_FS);
        fs.delete(writer).get();
        assertTrue(fs.list(writer).isEmpty());
        assertTrue(writer.mayHaveOrphanFiles);
        assertTrue(writer.getCacheEntry().fsInconsistent);
        assertTrue(Files.exists(Paths.get(dir, SEG_PREFIX + "0")));

        fs.setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.ASYNC);
        fs.fsync(writer).get();
        assertFalse(writer.mayHaveOrphanFiles);
        assertFalse(writer.getCacheEntry().fsInconsistent);
        assertTrue(fs.list(writer).isEmpty());
        assertNoSegmentOrIndexFiles(dir);
        fs.close(writer).get();
    }

    @Test
    public void testAtomicReplaceSurvivesNoFsToAsyncAndFlushesLatestGeneration() throws Exception {
        TailCacheFileSystem target = newFileSystem(noFsConfig());
        String p = path("atomic-latest");
        AsyncFile writer = target.open(p, AbstractStorageFile.OpenMode.WRITE, true, false, null).get();
        write(target, writer, bytes(1, 1, 1));
        long firstGen = writer.getCacheEntry().cacheGen;
        write(target, writer, bytes(9, 8, 7, 6));
        FileCacheEntry entry = writer.getCacheEntry();
        assertTrue(entry.cacheGen > firstGen);
        assertTrue(entry.fsInconsistent);
        assertFalse(Files.exists(Paths.get(p)));

        target.setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.ASYNC);
        target.fsync(writer).get();
        assertFalse(entry.fsInconsistent);
        assertEquals(entry.cacheGen, entry.writtenGen);
        assertArrayEquals(bytes(9, 8, 7, 6), Files.readAllBytes(Paths.get(p)));
        target.close(writer).get();
    }

    @Test
    public void testCloseSealsNoFsBufferedSegmentAndIndexesAfterReturnToAsync() throws Exception {
        TailCacheFileSystem target = newFileSystem(noFsConfig());
        String dir = path("close-seals");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile writer = target.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        write(target, writer, bytes(1, 2, 3, 4));
        AsyncFile idx = index(target, writer);
        write(target, idx, bytes(9, 8, 7));
        FileCacheEntry idxEntry = idx.getCacheEntry();
        assertTrue(idxEntry.isCacheDirty(false));

        target.setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.ASYNC);
        target.close(writer).get(5, TimeUnit.SECONDS);
        assertTrue(writer.closed);
        assertTrue(idx.closed);
        assertArrayEquals(bytes(1, 2, 3, 4), Files.readAllBytes(Paths.get(dir, SEG_PREFIX + "0")));
        assertArrayEquals(bytes(9, 8, 7), Files.readAllBytes(Paths.get(dir, IDX_PREFIX + "0")));
        assertEquals(3, idxEntry.writtenToFsOffset);
        assertEquals(0, target.getGlobalCommittedBytes());
    }

    // -------------------------------------------------------------------------
    // Idempotency and ASYNC failure handling (16)
    // -------------------------------------------------------------------------

    @Test
    public void testFileTruncateIoFailureRetrySameSizeForTailAndAtomic() throws Exception {
        runTruncateRetryScenario(false, "truncate-tail");
        runTruncateRetryScenario(true, "truncate-atomic");
    }

    private void runTruncateRetryScenario(boolean atomic, String name) throws Exception {
        String p = path(name);
        Files.write(Paths.get(p), bytes(1, 2, 3, 4, 5, 6));
        AsyncFile writer = fs.open(p, AbstractStorageFile.OpenMode.WRITE, atomic, false, null).get();
        if (atomic) {
            write(fs, writer, bytes(1, 2, 3, 4, 5, 6));
            fs.fsync(writer).get();
        }
        delegate.failOnce(Op.FILE_TRUNCATE, Failure.IO);
        expectFailure(() -> fs.truncate(writer, 3).get(5, TimeUnit.SECONDS));
        assertEquals(3, writer.getCacheEntry().cacheEndOffset);
        assertArrayEquals(bytes(1, 2, 3, 4, 5, 6), Files.readAllBytes(Paths.get(p)));

        fs.truncate(writer, 3).get(5, TimeUnit.SECONDS);
        assertArrayEquals(bytes(1, 2, 3), Files.readAllBytes(Paths.get(p)));
        assertEquals(2, delegate.count(Op.FILE_TRUNCATE));
        fs.close(writer).get();
        delegate.resetCounts();
    }

    @Test
    public void testFileDeleteIsRegisteredInFlightAndRetryWaitsForPriorDelete() throws Exception {
        String p = path("delete-barrier");
        AsyncFile writer = fs.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        write(fs, writer, bytes(1, 2, 3));
        fs.fsync(writer).get();
        delegate.resetCounts();
        delegate.hang(Op.FILE_DELETE);

        CompletableFuture<Void> first = fs.delete(writer);
        delegate.awaitEntered();
        CompletableFuture<Void> retry = CompletableFuture.runAsync(() -> {
            try {
                fs.delete(writer).get(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        Thread.sleep(50);
        assertFalse("retry must wait for the registered canonical delete", retry.isDone());
        assertEquals(1, delegate.count(Op.FILE_DELETE));

        delegate.releaseFault();
        first.get(5, TimeUnit.SECONDS);
        retry.get(5, TimeUnit.SECONDS);
        assertFalse(Files.exists(Paths.get(p)));
        assertEquals(2, delegate.count(Op.FILE_DELETE));
        fs.close(writer).get();
    }

    @Test
    public void testSegmentRollIndexFlushRetryableFailureLeavesMetadataUnchanged() throws Exception {
        String dir = path("roll-index-retry");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile writer = fs.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        write(fs, writer, bytes(1, 2, 3, 4));
        fs.fsync(writer).get();
        AsyncFile idx = index(fs, writer);
        write(fs, idx, bytes(7, 8, 9));
        delegate.failOnce(Op.INDEX_WRITE, Failure.EIO);

        expectFailure(() -> fs.roll(writer).get(5, TimeUnit.SECONDS));
        assertEquals(Collections.singletonList(0L), fs.list(writer));
        assertEquals(0, writer.openedSegmentStartOffset);
        assertTrue(idx.getCacheEntry().isCacheDirty(false));
        assertEquals(0, Files.size(Paths.get(dir, IDX_PREFIX + "0")));

        fs.roll(writer).get(5, TimeUnit.SECONDS);
        assertEquals(Arrays.asList(0L, 4L), fs.list(writer));
        assertArrayEquals(bytes(7, 8, 9), Files.readAllBytes(Paths.get(dir, IDX_PREFIX + "0")));
        assertEquals(2, delegate.count(Op.INDEX_WRITE));
        fs.close(writer).get();
    }

    @Test
    public void testSegmentRollChannelInitFailureRetryDoesNotRollTwice() throws Exception {
        String dir = path("roll-init-retry");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile writer = fs.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        write(fs, writer, bytes(1, 2, 3, 4));
        delegate.resetCounts();
        delegate.failOnce(Op.SEG_INIT_CHANNELS, Failure.IO);

        expectFailure(() -> fs.roll(writer).get(5, TimeUnit.SECONDS));
        assertEquals(Arrays.asList(0L, 4L), fs.list(writer));
        assertEquals(4, writer.openedSegmentStartOffset);
        assertNull(writer.currentSegmentChannel);

        fs.roll(writer).get(5, TimeUnit.SECONDS);
        assertEquals("empty new tail makes retry metadata a no-op",
                Arrays.asList(0L, 4L), fs.list(writer));
        assertNotNull(writer.currentSegmentChannel);
        assertEquals(2, delegate.count(Op.SEG_ROLL_METADATA));
        assertEquals(2, delegate.count(Op.SEG_INIT_CHANNELS));
        fs.close(writer).get();
    }

    @Test
    public void testSegmentCloseFlushesDirtyCurrentIndexes() throws Exception {
        String dir = path("close-index");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile writer = fs.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        write(fs, writer, bytes(1, 2, 3, 4));
        AsyncFile idx = index(fs, writer);
        write(fs, idx, bytes(5, 6, 7));
        FileCacheEntry idxEntry = idx.getCacheEntry();
        assertTrue(idxEntry.isCacheDirty(false));

        fs.close(writer).get(5, TimeUnit.SECONDS);
        assertTrue(writer.closed);
        assertArrayEquals(bytes(1, 2, 3, 4), Files.readAllBytes(Paths.get(dir, SEG_PREFIX + "0")));
        assertArrayEquals(bytes(5, 6, 7), Files.readAllBytes(Paths.get(dir, IDX_PREFIX + "0")));
        assertEquals(3, idxEntry.writtenToFsOffset);
        assertTrue(delegate.count(Op.INDEX_WRITE) > 0);
    }

    @Test
    public void testSegmentCloseIndexFlushTimeoutOrEioLeavesOpenAndRetrySucceeds() throws Exception {
        String dir = path("close-index-eio");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile writer = fs.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        write(fs, writer, bytes(1, 2, 3));
        AsyncFile idx = index(fs, writer);
        write(fs, idx, bytes(9, 9));
        delegate.failOnce(Op.INDEX_WRITE, Failure.EIO);

        expectFailure(() -> fs.close(writer).get(5, TimeUnit.SECONDS));
        assertFalse("retryable index flush failure must leave the segment handle open", writer.closed);
        assertFalse(idx.closed);
        assertTrue(idx.getCacheEntry().isCacheDirty(false));

        fs.close(writer).get(5, TimeUnit.SECONDS);
        assertTrue(writer.closed);
        assertArrayEquals(bytes(9, 9), Files.readAllBytes(Paths.get(dir, IDX_PREFIX + "0")));
        assertEquals(2, delegate.count(Op.INDEX_WRITE));
    }

    @Test
    public void testFileWriteIoFailureIsRecoveredByFsyncWithoutReappend() throws Exception {
        TailCacheFileSystem target = newFileSystem(baseConfig().setWriteBatchBytes(1));
        String p = path("file-write-recover");
        AsyncFile writer = target.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        delegate.failOnce(Op.FILE_WRITE, Failure.IO);
        write(target, writer, bytes(1, 2, 3, 4));
        waitUntil("failed write attempt must finish", () -> delegate.count(Op.FILE_WRITE) == 1);
        assertEquals(4, writer.getCacheEntry().cacheEndOffset);
        assertEquals(0, writer.getCacheEntry().writtenToFsOffset);

        target.fsync(writer).get(5, TimeUnit.SECONDS);
        assertArrayEquals(bytes(1, 2, 3, 4), Files.readAllBytes(Paths.get(p)));
        assertEquals(4, writer.getCacheEntry().writtenToFsOffset);
        assertEquals("the logical append is not replayed", 2, delegate.count(Op.FILE_WRITE));
        target.close(writer).get();
    }

    @Test
    public void testSegmentWriteIoFailureIsRecoveredByFsyncWithoutDuplicate() throws Exception {
        TailCacheFileSystem target = newFileSystem(baseConfig().setWriteBatchBytes(1));
        String dir = path("segment-write-recover");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile writer = target.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        delegate.failOnce(Op.SEG_WRITE, Failure.IO);
        write(target, writer, bytes(1, 2, 3, 4));
        waitUntil("failed segment write attempt must finish", () -> delegate.count(Op.SEG_WRITE) == 1);
        assertEquals(4, writer.getCacheEntry().cacheEndOffset);
        assertEquals(0, writer.getCacheEntry().writtenToFsOffset);

        target.fsync(writer).get(5, TimeUnit.SECONDS);
        assertArrayEquals(bytes(1, 2, 3, 4), Files.readAllBytes(Paths.get(dir, SEG_PREFIX + "0")));
        assertEquals(4, writer.getCacheEntry().writtenToFsOffset);
        assertEquals(2, delegate.count(Op.SEG_WRITE));
        target.close(writer).get();
    }

    @Test
    public void testAtomicRestoreFailureRetrySamePayloadUsesLatestGeneration() throws Exception {
        TailCacheFileSystem target = newFileSystem(noFsConfig());
        String p = path("atomic-restore-retry");
        AsyncFile writer = target.open(p, AbstractStorageFile.OpenMode.WRITE, true, false, null).get();
        write(target, writer, bytes(1, 1, 1));
        target.setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.ASYNC);
        delegate.failOnce(Op.FILE_SIZE, Failure.IO);
        byte[] latest = bytes(8, 7, 6, 5);
        long before = writer.getCacheEntry().cacheGen;

        expectFailure(() -> target.write(writer, buf(latest, true)).get(5, TimeUnit.SECONDS));
        long failedGen = writer.getCacheEntry().cacheGen;
        assertTrue(failedGen > before);
        assertArrayEquals(latest, read(target.read(writer, latest.length, 0).get()));
        assertTrue(writer.getCacheEntry().fsInconsistent);

        write(target, writer, latest);
        target.fsync(writer).get();
        assertTrue(writer.getCacheEntry().cacheGen > failedGen);
        assertEquals(writer.getCacheEntry().cacheGen, writer.getCacheEntry().writtenGen);
        assertArrayEquals(latest, Files.readAllBytes(Paths.get(p)));
        target.close(writer).get();
    }

    @Test
    public void testFileDeleteFailureRetrySameHandleIsIdempotent() throws Exception {
        String p = path("delete-retry");
        AsyncFile writer = fs.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        write(fs, writer, bytes(1, 2, 3));
        fs.fsync(writer).get();
        delegate.failOnce(Op.FILE_DELETE, Failure.IO);

        expectFailure(() -> fs.delete(writer).get(5, TimeUnit.SECONDS));
        assertEquals(0, writer.getCacheEntry().cacheEndOffset);
        assertArrayEquals(bytes(1, 2, 3), Files.readAllBytes(Paths.get(p)));
        assertFalse(writer.closed);

        fs.delete(writer).get(5, TimeUnit.SECONDS);
        assertFalse(Files.exists(Paths.get(p)));
        assertEquals(2, delegate.count(Op.FILE_DELETE));
        fs.close(writer).get();
    }

    @Test
    public void testDeleteSegmentsPartialIoFailureRetryIsIdempotent() throws Exception {
        String dir = path("delete-prefix-partial");
        AsyncSegmentFile writer = buildThreeSegments(fs, dir);
        delegate.partialUnlinkOnce();

        expectFailure(() -> fs.deleteSegments(writer, Arrays.asList(0L, 4L)).get(5, TimeUnit.SECONDS));
        assertEquals(Collections.singletonList(8L), fs.list(writer));
        assertTrue(writer.mayHaveOrphanFiles);
        assertFalse("the injected failure happens after one real unlink",
                Files.exists(Paths.get(dir, SEG_PREFIX + "0")));
        assertTrue(Files.exists(Paths.get(dir, SEG_PREFIX + "4")));
        assertArrayEquals(bytes(9, 10, 11, 12), Files.readAllBytes(Paths.get(dir, SEG_PREFIX + "8")));

        fs.deleteSegments(writer, Arrays.asList(0L, 4L)).get(5, TimeUnit.SECONDS);
        assertEquals(Collections.singletonList(8L), fs.list(writer));
        assertFalse(writer.mayHaveOrphanFiles);
        assertFalse(Files.exists(Paths.get(dir, SEG_PREFIX + "4")));
        assertTrue(Files.exists(Paths.get(dir, SEG_PREFIX + "8")));
        assertEquals(2, delegate.count(Op.SEG_UNLINK));
        fs.close(writer).get();
    }

    @Test
    public void testDeleteAllSegmentsPartialIoFailureRetryIsIdempotent() throws Exception {
        String dir = path("delete-all-partial");
        AsyncSegmentFile writer = buildThreeSegments(fs, dir);
        delegate.partialUnlinkOnce();

        expectFailure(() -> fs.delete(writer).get(5, TimeUnit.SECONDS));
        assertTrue(fs.list(writer).isEmpty());
        assertTrue(writer.mayHaveOrphanFiles);
        assertFalse(Files.exists(Paths.get(dir, SEG_PREFIX + "0")));
        assertTrue(Files.exists(Paths.get(dir, SEG_PREFIX + "4")));
        assertEquals(0, writer.getCacheEntry().cacheEndOffset);

        fs.delete(writer).get(5, TimeUnit.SECONDS);
        assertTrue(fs.list(writer).isEmpty());
        assertFalse(writer.mayHaveOrphanFiles);
        assertNoSegmentOrIndexFiles(dir);
        assertEquals(1, delegate.count(Op.SEG_DELETE_METADATA));
        assertEquals(2, delegate.count(Op.SEG_UNLINK));
        fs.close(writer).get();
    }

    @Test
    public void testSegmentTruncateChannelFailureRetrySameOffsetIsIdempotent() throws Exception {
        String dir = path("truncate-channel-retry");
        AsyncSegmentFile writer = buildThreeSegments(fs, dir);
        delegate.failOnce(Op.SEG_TRUNCATE_CHANNEL, Failure.IO);

        expectFailure(() -> fs.truncate(writer, 6).get(5, TimeUnit.SECONDS));
        assertEquals(Arrays.asList(0L, 4L), fs.list(writer));
        assertEquals(6, writer.getCacheEntry().cacheEndOffset);
        assertTrue(writer.mayHaveOrphanFiles);
        assertEquals(4, Files.size(Paths.get(dir, SEG_PREFIX + "4")));
        assertTrue(Files.exists(Paths.get(dir, SEG_PREFIX + "8")));

        fs.truncate(writer, 6).get(5, TimeUnit.SECONDS);
        assertEquals(Arrays.asList(0L, 4L), fs.list(writer));
        assertEquals(2, Files.size(Paths.get(dir, SEG_PREFIX + "4")));
        assertFalse(Files.exists(Paths.get(dir, SEG_PREFIX + "8")));
        assertFalse(writer.mayHaveOrphanFiles);
        assertEquals(2, delegate.count(Op.SEG_TRUNCATE_CHANNEL));
        fs.close(writer).get();
    }

    @Test
    public void testSegmentTruncatePartialUnlinkFailureRetrySameOffsetIsIdempotent() throws Exception {
        String dir = path("truncate-unlink-retry");
        AsyncSegmentFile writer = buildThreeSegments(fs, dir);
        delegate.partialUnlinkOnce();

        expectFailure(() -> fs.truncate(writer, 6).get(5, TimeUnit.SECONDS));
        assertEquals(Arrays.asList(0L, 4L), fs.list(writer));
        assertEquals(2, Files.size(Paths.get(dir, SEG_PREFIX + "4")));
        assertFalse("first dropped target was physically removed",
                Files.exists(Paths.get(dir, SEG_PREFIX + "8")));
        assertTrue("its index remains as orphan debt",
                Files.exists(Paths.get(dir, IDX_PREFIX + "8")));
        assertTrue(writer.mayHaveOrphanFiles);

        fs.truncate(writer, 6).get(5, TimeUnit.SECONDS);
        assertEquals(2, Files.size(Paths.get(dir, SEG_PREFIX + "4")));
        assertFalse(Files.exists(Paths.get(dir, IDX_PREFIX + "8")));
        assertFalse(writer.mayHaveOrphanFiles);
        assertEquals(2, delegate.count(Op.SEG_UNLINK));
        fs.close(writer).get();
    }

    @Test
    public void testFileRestorePartialIoFailureRetryIsIdempotent() throws Exception {
        String p = path("file-restore-partial");
        AsyncFile writer = fs.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        write(fs, writer, bytes(1, 2, 3, 4, 5, 6));
        fs.fsync(writer).get();
        fs.setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.NO_FS);
        fs.truncate(writer, 3).get();
        write(fs, writer, bytes(9, 10));
        FileCacheEntry entry = writer.getCacheEntry();
        assertTrue(entry.fsInconsistent);

        fs.setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.ASYNC);
        delegate.failOnce(Op.FILE_SIZE, Failure.IO);
        fs.fsync(writer).get(5, TimeUnit.SECONDS);
        assertTrue("truncate completed but size verification failed", entry.fsInconsistent);
        assertArrayEquals(bytes(1, 2, 3), Files.readAllBytes(Paths.get(p)));

        fs.fsync(writer).get(5, TimeUnit.SECONDS);
        assertFalse(entry.fsInconsistent);
        assertEquals(5, entry.writtenToFsOffset);
        assertArrayEquals(bytes(1, 2, 3, 9, 10), Files.readAllBytes(Paths.get(p)));
        assertTrue(delegate.count(Op.FILE_TRUNCATE) >= 2);
        fs.close(writer).get();
    }

    @Test
    public void testIndexNoSpacePropagatesToSegmentAndCloseStillReleases() throws Exception {
        TailCacheFileSystem target = newFileSystem(baseConfig().setWriteBatchBytes(1));
        String dir = path("index-enospc");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile writer = target.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        write(target, writer, bytes(1, 2, 3));
        target.fsync(writer).get();
        AsyncFile idx = index(target, writer);
        FileCacheEntry idxEntry = idx.getCacheEntry();
        delegate.failOnce(Op.INDEX_WRITE, Failure.NO_SPACE);
        write(target, idx, bytes(7, 8));
        waitUntil("index ENOSPC must be latched", () -> idx.noSpaceFailure != null);

        target.close(writer).get(5, TimeUnit.SECONDS);
        assertTrue(writer.closed);
        assertTrue(idx.closed);
        assertNotNull(writer.noSpaceFailure);
        assertTrue(StorageUtil.isNoSpace(writer.noSpaceFailure));
        assertEquals("unflushable index cache is released on close", 0, idxEntry.bodySizeBytes);
        assertEquals(0, target.getGlobalCommittedBytes());
        Path idxPath = Paths.get(dir, IDX_PREFIX + "0");
        assertTrue(!Files.exists(idxPath) || Files.size(idxPath) == 0);
    }

    // -------------------------------------------------------------------------
    // Pure NO_FS (6)
    // -------------------------------------------------------------------------

    @Test
    public void testPureNoFsRegularFileTruncateIsCacheOnly() throws Exception {
        TailCacheFileSystem target = newFileSystem(noFsConfig());
        String p = path("pure-truncate");
        AsyncFile writer = target.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        write(target, writer, bytes(1, 2, 3, 4, 5, 6));
        target.truncate(writer, 3).get();

        FileCacheEntry entry = writer.getCacheEntry();
        assertEquals(3, entry.cacheEndOffset);
        assertEquals(0, entry.writtenToFsOffset);
        assertTrue(entry.fsInconsistent);
        assertArrayEquals(bytes(1, 2, 3), read(target.read(writer, 3, 0).get()));
        assertFalse(Files.exists(Paths.get(p)));
        assertEquals(0, delegate.count(Op.FILE_TRUNCATE));
        target.close(writer).get();
    }

    @Test
    public void testPureNoFsPositionIsMetadataOnlyForFileAndSegment() throws Exception {
        TailCacheFileSystem target = newFileSystem(noFsConfig());
        String p = path("position-file");
        AsyncFile fileWriter = target.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        write(target, fileWriter, bytes(1, 2, 3, 4));
        AsyncFile fileReader = target.open(p, AbstractStorageFile.OpenMode.READ, false, false, null).get();
        target.position(fileReader, 2).get();
        assertEquals(2, fileReader.position);
        assertNull(fileReader.channel);
        assertArrayEquals(bytes(3, 4), read(target.read(fileReader, 2).get()));

        String dir = path("position-segment");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile segWriter = target.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        write(target, segWriter, bytes(5, 6, 7, 8));
        target.roll(segWriter).get();
        write(target, segWriter, bytes(9, 10));
        AsyncSegmentFile segReader = target.open(dir, SEG_PREFIX, INDEX_PREFIXES, false, null).get();
        target.position(segReader, 4).get();
        assertEquals(4, segReader.position);
        assertEquals(4, segReader.openedSegmentStartOffset);
        assertNull(segReader.currentSegmentChannel);
        assertArrayEquals(bytes(9, 10), read(target.read(segReader, 2).get()));
        assertFalse(Files.exists(Paths.get(p)));
        assertFalse(Files.exists(Paths.get(dir, SEG_PREFIX + "0")));

        target.close(fileReader).get();
        target.close(fileWriter).get();
        target.close(segReader).get();
        target.close(segWriter).get();
    }

    @Test
    public void testPureNoFsSegmentTransferToUsesCacheAndHonorsReadableWindow() throws Exception {
        TailCacheFileSystemConfig config = noFsConfig()
                .setMaxCacheSizeBytes(200)
                .setPerFileCacheLimits(10 * 1024, 1, CHUNK_SIZE)
                .setWatermarkRatios(0.3, 0.5);
        TailCacheFileSystem target = newFileSystem(config);
        String dir = path("transfer-window");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile writer = target.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        byte[] chunk = new byte[(int) CHUNK_SIZE];
        Arrays.fill(chunk, (byte) 3);
        write(target, writer, chunk);
        write(target, writer, chunk);
        write(target, writer, chunk);
        assertEquals(CHUNK_SIZE, writer.getCacheEntry().cacheStartOffset);

        try {
            target.transferTo(writer, 0, 8, new ByteArrayChannel());
            fail("evicted range is unreadable without a backing FS");
        } catch (CannotReadPositionInNoFsException expected) {
            assertEquals(CHUNK_SIZE, writer.getCacheEntry().cacheStartOffset);
        }
        ByteArrayChannel channel = new ByteArrayChannel();
        assertEquals(8, (long) target.transferTo(writer, CHUNK_SIZE, 8, channel).get());
        assertArrayEquals(new byte[]{3, 3, 3, 3, 3, 3, 3, 3}, channel.toByteArray());
        assertEquals(0, delegate.count(Op.SEG_TRANSFER));
        assertFalse(Files.exists(Paths.get(dir, SEG_PREFIX + "0")));
        target.close(writer).get();
    }

    @Test
    public void testPureNoFsSegmentFsyncIsNoOpAndCloseReleasesIndexCaches() throws Exception {
        TailCacheFileSystem target = newFileSystem(noFsConfig());
        String dir = path("nofs-fsync-close");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile writer = target.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        write(target, writer, bytes(1, 2, 3));
        AsyncFile idx = index(target, writer);
        write(target, idx, bytes(4, 5));
        FileCacheEntry idxEntry = idx.getCacheEntry();
        target.fsync(writer).get();

        assertEquals(0, writer.getCacheEntry().writtenToFsOffset);
        assertEquals(0, idxEntry.writtenToFsOffset);
        assertTrue(idxEntry.isCacheDirty(false));
        assertEquals(0, delegate.count(Op.SEG_FSYNC));
        assertEquals(0, delegate.count(Op.INDEX_FSYNC));

        target.close(writer).get();
        assertTrue(writer.closed);
        assertTrue(idx.closed);
        assertEquals(0, idxEntry.bodySizeBytes);
        assertEquals(0, target.getGlobalCommittedBytes());
        assertNoSegmentOrIndexFiles(dir);
    }

    @Test
    public void testPureNoFsReadersExposeOnlySharedInMemoryState() throws Exception {
        TailCacheFileSystem target = newFileSystem(noFsConfig());
        String p = path("shared-file");
        AsyncFile fileWriter = target.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        write(target, fileWriter, bytes(1, 2));
        AsyncFile fileReader = target.open(p, AbstractStorageFile.OpenMode.READ, false, false, null).get();
        assertSame(fileWriter.getCacheEntry(), fileReader.getCacheEntry());
        assertArrayEquals(bytes(1, 2), read(target.read(fileReader, 2, 0).get()));
        write(target, fileWriter, bytes(3, 4));
        assertArrayEquals(bytes(1, 2, 3, 4), read(target.read(fileReader, 4, 0).get()));
        target.close(fileWriter).get();
        assertArrayEquals("reader retains the shared cache after writer close",
                bytes(1, 2, 3, 4), read(target.read(fileReader, 4, 0).get()));

        String dir = path("shared-segment");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile segWriter = target.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        write(target, segWriter, bytes(5, 6));
        AsyncSegmentFile segReader = target.open(dir, SEG_PREFIX, INDEX_PREFIXES, false, null).get();
        assertSame(segWriter.getCacheEntry(), segReader.getCacheEntry());
        write(target, segWriter, bytes(7, 8));
        assertArrayEquals(bytes(5, 6, 7, 8), read(target.read(segReader, 4, 0).get()));
        assertFalse(Files.exists(Paths.get(p)));
        assertFalse(Files.exists(Paths.get(dir, SEG_PREFIX + "0")));

        target.close(fileReader).get();
        target.close(segReader).get();
        target.close(segWriter).get();
        assertEquals(0, target.getGlobalCommittedBytes());
    }

    @Test
    public void testPureNoFsSegmentTruncateOutsideCachedRangeResetsMetadataAndCache() throws Exception {
        TailCacheFileSystem target = newFileSystem(noFsConfig());
        String dir = path("truncate-outside");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile writer = target.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        write(target, writer, bytes(1, 2, 3, 4));
        target.roll(writer).get();
        write(target, writer, bytes(5, 6, 7, 8));
        assertEquals(Arrays.asList(0L, 4L), target.list(writer));

        target.truncate(writer, 100).get();
        SegmentFileCacheEntry entry = writer.getCacheEntry();
        assertEquals(Collections.singletonList(100L), target.list(writer));
        assertEquals(100, writer.openedSegmentStartOffset);
        assertEquals(Long.MAX_VALUE, writer.openedSegmentEndOffset);
        assertEquals(100, entry.cacheStartOffset);
        assertEquals(100, entry.cacheEndOffset);
        assertEquals(100, entry.writtenToFsOffset);
        assertTrue(entry.chunks.isEmpty());
        assertTrue(entry.fsInconsistent);
        assertFalse(Files.exists(Paths.get(dir, SEG_PREFIX + "100")));
        target.close(writer).get();
    }

    private void assertNoSegmentOrIndexFiles(String dir) throws IOException {
        File[] files = new File(dir).listFiles();
        if (files == null) return;
        for (File file : files) {
            assertFalse("unexpected storage file: " + file,
                    file.getName().startsWith(SEG_PREFIX) || file.getName().startsWith(IDX_PREFIX));
        }
    }

    @FunctionalInterface
    private interface CheckedRunnable {
        void run() throws Exception;
    }

    private enum Op {
        FILE_WRITE,
        INDEX_WRITE,
        SEG_WRITE,
        FILE_FSYNC,
        INDEX_FSYNC,
        SEG_FSYNC,
        FILE_SIZE,
        FILE_TRUNCATE,
        FILE_DELETE,
        SEG_ROLL_METADATA,
        SEG_INIT_CHANNELS,
        SEG_TRUNCATE_CHANNEL,
        SEG_DELETE_METADATA,
        SEG_UNLINK,
        SEG_TRANSFER
    }

    private enum Failure { IO, EIO, NO_SPACE }

    private static final class InstrumentedDelegate extends AsyncTFSBasedFileSystem {
        private final Map<Op, AtomicInteger> counts = new EnumMap<>(Op.class);
        private volatile Op faultOp;
        private volatile Failure failure;
        private volatile boolean hang;
        private volatile boolean partialUnlink;
        private volatile CountDownLatch entered = new CountDownLatch(0);
        private volatile CountDownLatch gate = new CountDownLatch(0);

        InstrumentedDelegate(ExecutorService ioExecutor) {
            super(ioExecutor, Long.MAX_VALUE / 2, Long.MAX_VALUE / 2_000_000L);
            for (Op op : Op.values()) counts.put(op, new AtomicInteger());
        }

        int count(Op op) {
            return counts.get(op).get();
        }

        void resetCounts() {
            for (AtomicInteger count : counts.values()) count.set(0);
        }

        void failOnce(Op op, Failure failure) {
            releaseFault();
            this.faultOp = op;
            this.failure = failure;
            this.hang = false;
            this.entered = new CountDownLatch(1);
        }

        void hang(Op op) {
            releaseFault();
            this.faultOp = op;
            this.failure = null;
            this.hang = true;
            this.entered = new CountDownLatch(1);
            this.gate = new CountDownLatch(1);
        }

        void partialUnlinkOnce() {
            releaseFault();
            this.partialUnlink = true;
        }

        void awaitEntered() throws InterruptedException {
            assertTrue("fault point was not entered", entered.await(5, TimeUnit.SECONDS));
        }

        void releaseFault() {
            CountDownLatch oldGate = gate;
            gate = new CountDownLatch(0);
            hang = false;
            faultOp = null;
            failure = null;
            oldGate.countDown();
        }

        private void before(Op op) {
            counts.get(op).incrementAndGet();
            if (faultOp != op) return;
            entered.countDown();
            if (hang) {
                try {
                    if (!gate.await(30, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("fault gate timed out for " + op);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
                return;
            }
            Failure selected = failure;
            faultOp = null;
            failure = null;
            if (selected == Failure.EIO) {
                throw new EIOException(new IOException("injected EIO for " + op));
            }
            IOException cause = selected == Failure.NO_SPACE
                    ? new IOException("No space left on device")
                    : new IOException("injected IO failure for " + op);
            throw StorageUtil.wrapIOException(cause);
        }

        private void beforeWrite(Op op, ByteBuf data) {
            try {
                before(op);
            } catch (RuntimeException e) {
                data.release();
                throw e;
            }
        }

        @Override
        public long writeSync(AsyncFile file, ByteBuf data) {
            beforeWrite(file instanceof AsyncIndexFile ? Op.INDEX_WRITE : Op.FILE_WRITE, data);
            return super.writeSync(file, data);
        }

        @Override
        public long writeSync(AsyncSegmentFile file, ByteBuf data) {
            beforeWrite(Op.SEG_WRITE, data);
            return super.writeSync(file, data);
        }

        @Override
        public void fsyncSync(AsyncFile file) {
            before(file instanceof AsyncIndexFile ? Op.INDEX_FSYNC : Op.FILE_FSYNC);
            super.fsyncSync(file);
        }

        @Override
        public void fsyncSync(AsyncSegmentFile file) {
            before(Op.SEG_FSYNC);
            super.fsyncSync(file);
        }

        @Override
        public long sizeSync(AsyncFile file) {
            before(Op.FILE_SIZE);
            return super.sizeSync(file);
        }

        @Override
        public void truncateSync(AsyncFile file, long size) {
            before(Op.FILE_TRUNCATE);
            super.truncateSync(file, size);
        }

        @Override
        public void deleteSync(String path) {
            before(Op.FILE_DELETE);
            super.deleteSync(path);
        }

        @Override
        public List<FileChannel> rollMetadataSync(AsyncSegmentFile file, long currentSegmentSize, boolean noFs) {
            before(Op.SEG_ROLL_METADATA);
            return super.rollMetadataSync(file, currentSegmentSize, noFs);
        }

        @Override
        public void initCurrentChannelsSync(AsyncSegmentFile file) {
            before(Op.SEG_INIT_CHANNELS);
            super.initCurrentChannelsSync(file);
        }

        @Override
        public void truncateLastSegmentChannel(AsyncSegmentFile file, long offset) {
            before(Op.SEG_TRUNCATE_CHANNEL);
            super.truncateLastSegmentChannel(file, offset);
        }

        @Override
        public List<FileChannel> deleteMetadataSync(AsyncSegmentFile file) {
            before(Op.SEG_DELETE_METADATA);
            return super.deleteMetadataSync(file);
        }

        @Override
        public void deleteSegmentsIo(AsyncSegmentFile file, long[] droppedOffsets) {
            before(Op.SEG_UNLINK);
            if (partialUnlink) {
                partialUnlink = false;
                if (droppedOffsets.length > 0) {
                    try {
                        Files.deleteIfExists(file.segmentPath(droppedOffsets[0]));
                    } catch (IOException e) {
                        throw StorageUtil.wrapIOException(e);
                    }
                }
                throw StorageUtil.wrapIOException(new IOException("injected after first physical unlink"));
            }
            super.deleteSegmentsIo(file, droppedOffsets);
        }

        @Override
        public long transferToSync(AsyncSegmentFile file, long offset, long count,
                WritableByteChannel target) {
            before(Op.SEG_TRANSFER);
            return super.transferToSync(file, offset, count, target);
        }
    }

    private static final class ByteArrayChannel implements WritableByteChannel {
        private final java.io.ByteArrayOutputStream output = new java.io.ByteArrayOutputStream();
        private boolean open = true;

        @Override
        public int write(java.nio.ByteBuffer src) {
            int length = src.remaining();
            byte[] bytes = new byte[length];
            src.get(bytes);
            output.write(bytes, 0, bytes.length);
            return length;
        }

        @Override
        public boolean isOpen() {
            return open;
        }

        @Override
        public void close() {
            open = false;
        }

        byte[] toByteArray() {
            return output.toByteArray();
        }
    }
}
