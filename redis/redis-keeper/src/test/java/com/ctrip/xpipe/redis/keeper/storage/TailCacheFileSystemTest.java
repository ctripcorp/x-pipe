package com.ctrip.xpipe.redis.keeper.storage;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.ctrip.xpipe.tuple.Pair;

import static org.junit.Assert.*;

public class TailCacheFileSystemTest {

    private static final Logger logger = LoggerFactory.getLogger(TailCacheFileSystemTest.class);

    private static final long CHUNK_SIZE = 64;

    private Path tempDir;
    private TrackingExecutor ioExecutor;
    private RecordingDelegate delegate;
    private TailCacheFileSystem tcf;
    // Instances created by individual tests; shut down in tearDown.
    private final List<TailCacheFileSystem> extraFileSystems = new ArrayList<>();
    // Faulty delegates created by individual tests; released in tearDown so a test that fails
    // mid-way cannot leave a gate closed and block the shared io executor.
    private final List<FaultyDelegate> faultyDelegates = new ArrayList<>();

    @Before
    public void setUp() throws Exception {
        tempDir = Files.createTempDirectory("tailcache-test-");
        ioExecutor = new TrackingExecutor(Executors.newCachedThreadPool());
        delegate = new RecordingDelegate(ioExecutor);

        TailCacheFileSystemConfig config = new TailCacheFileSystemConfig();
        config.setPerFileCacheLimits(10 * 1024, 1, CHUNK_SIZE);
        config.setMaxCacheSizeBytes(100 * 1024);
        config.setWriteBatchBytes(128);
        config.setIoWaitTimeoutMs(5000);
        config.setExpectedMinRetentionMs(0);
        config.setEvictScanIntervalMs(60_000);
        config.setWatermarkRatios(0.5, 0.8);
        config.setMaxEvictRatioPerWrite(0.5);

        tcf = new TailCacheFileSystem(delegate, config, ioExecutor);
    }

    @After
    public void tearDown() throws Exception {
        // Unblock anything still parked on a fault gate before shutting the systems down.
        for (FaultyDelegate faulty : faultyDelegates) {
            faulty.release();
        }
        faultyDelegates.clear();
        // Every instance owns a private evictExecutor that keeps rescheduling runEvictScan, so an
        // instance that is never shut down leaks a scheduled thread past the end of the test.
        // Shutdown has to happen here rather than inside the test: shutdown() cascades into
        // delegate.shutdown(), which closes the ioExecutor shared by all instances below.
        for (TailCacheFileSystem extra : extraFileSystems) {
            extra.shutdown();
        }
        extraFileSystems.clear();
        tcf.shutdown();
        deleteRecursively(tempDir.toFile());
    }

    // Use instead of `new TailCacheFileSystem(...)` so tearDown shuts the instance down.
    private TailCacheFileSystem newTcf(TailCacheFileSystemConfig config) {
        return newTcf(delegate, config);
    }

    private TailCacheFileSystem newTcf(AsyncFileSystem backingFs, TailCacheFileSystemConfig config) {
        TailCacheFileSystem created = new TailCacheFileSystem(backingFs, config, ioExecutor);
        extraFileSystems.add(created);
        return created;
    }

    private TailCacheFileSystemConfig baseConfig() {
        TailCacheFileSystemConfig config = new TailCacheFileSystemConfig();
        config.setPerFileCacheLimits(10 * 1024, 1, CHUNK_SIZE);
        config.setMaxCacheSizeBytes(100 * 1024);
        config.setWriteBatchBytes(128);
        config.setIoWaitTimeoutMs(5000);
        config.setExpectedMinRetentionMs(0);
        config.setEvictScanIntervalMs(60_000);
        config.setWatermarkRatios(0.5, 0.8);
        config.setMaxEvictRatioPerWrite(0.5);
        return config;
    }

    private TailCacheFileSystem newNoFsTcf() {
        return newTcf(baseConfig().setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.NO_FS));
    }

    private byte[] bytes(int... values) {
        byte[] result = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            result[i] = (byte) values[i];
        }
        return result;
    }

    private static void deleteRecursively(File f) {
        if (f.isDirectory()) {
            File[] children = f.listFiles();
            if (children != null) {
                for (File c : children) deleteRecursively(c);
            }
        }
        f.delete();
    }

    private String path(String name) {
        return tempDir.resolve(name).toString();
    }

    private ByteBuf bufOf(byte[] data) {
        return Unpooled.wrappedBuffer(data);
    }

    private byte[] readBytes(ByteBuf buf) {
        try {
            byte[] result = new byte[buf.readableBytes()];
            buf.readBytes(result);
            return result;
        } finally {
            buf.release();
        }
    }

    private byte[] readFileSync(String filePath) throws IOException {
        return Files.readAllBytes(Paths.get(filePath));
    }

    private void writeFileSync(String filePath, byte[] data) throws IOException {
        Files.write(Paths.get(filePath), data);
    }

    private byte[] readTcfSync(AsyncFile file, long length) throws Exception {
        return readBytes(tcf.read(file, length).get(5, TimeUnit.SECONDS));
    }

    private long writeTcfSync(AsyncFile file, byte[] data) throws Exception {
        return tcf.write(file, bufOf(data)).get(5, TimeUnit.SECONDS);
    }

    private void awaitAll() throws Exception {
        ioExecutor.awaitAll();
    }

    // =========================================================================
    // A. AsyncFile cache write/read
    // =========================================================================

    @Test
    public void testWriteThenCloseThenReopen() throws Exception {
        String p = path("file2");
        AsyncFile writer = tcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        writeTcfSync(writer, new byte[]{10, 20, 30});
        // Verify cache has data before close
        FileCacheEntry entry = writer.getCacheEntry();
        assertTrue(entry.isInitialized());
        assertEquals(3, entry.cacheEndOffset);

        tcf.close(writer).get(5, TimeUnit.SECONDS);

        // Close ensures data is on disk
        assertArrayEquals(new byte[]{10, 20, 30}, readFileSync(p));

        // Reopen reader and verify correct data
        AsyncFile reader = tcf.open(p, AbstractStorageFile.OpenMode.READ, false, false, null).get();
        try {
            byte[] data = readTcfSync(reader, 3);
            assertArrayEquals(new byte[]{10, 20, 30}, data);
            // Reader's entry is initialized to the on-disk range
            FileCacheEntry readerEntry = reader.getCacheEntry();
            assertTrue(readerEntry.isInitialized());
            assertEquals(3, readerEntry.cacheEndOffset);
        } finally {
            tcf.close(reader).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testReadFromExistingFile() throws Exception {
        String p = path("file3");
        writeFileSync(p, new byte[]{5, 6, 7, 8});

        AsyncFile reader = tcf.open(p, AbstractStorageFile.OpenMode.READ, false, false, null).get();
        try {
            // TAIL_CACHE reader does not preload chunks; read goes to delegate
            byte[] data = readTcfSync(reader, 4);
            assertArrayEquals(new byte[]{5, 6, 7, 8}, data);
            // Verify cache entry initialized with correct range
            FileCacheEntry entry = reader.getCacheEntry();
            assertTrue(entry.isInitialized());
            assertEquals(4, entry.cacheEndOffset);
        } finally {
            tcf.close(reader).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testWriteBatching() throws Exception {
        // writeBatchBytes=128 (from setUp). Writes below threshold stay in cache,
        // no ioExecutor task scheduled, no delegate write.
        // Once pending reaches threshold, ioExecutor is scheduled and data is flushed.

        String p = path("file4");
        AsyncFile writer = tcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        FileCacheEntry entry = writer.getCacheEntry();

        // Write 50 bytes — pending(50) == writeSize(50), 50 < writeBatchBytes(128) → no IO
        delegate.reset();
        int tasksBefore = ioExecutor.submittedCount();
        writeTcfSync(writer, new byte[50]);
        assertEquals("no task submitted", tasksBefore, ioExecutor.submittedCount());
        assertEquals(0, delegate.fileWriteCount);
        assertEquals(50, entry.cacheEndOffset);
        assertEquals(0, entry.writtenToFsOffset);
        assertEquals(0, new java.io.File(p).length());

        // Write 60 bytes — pending(110) != writeSize(60), buildWriteBufAfterInFlight: 110 < 128 → no IO
        delegate.reset();
        tasksBefore = ioExecutor.submittedCount();
        writeTcfSync(writer, new byte[60]);
        assertEquals("no task submitted", tasksBefore, ioExecutor.submittedCount());
        assertEquals(0, delegate.fileWriteCount);
        assertEquals(110, entry.cacheEndOffset);
        assertEquals(0, entry.writtenToFsOffset);

        // Write 100 bytes — cacheEnd → 210, pending(210) != writeSize(100)
        // → buildWriteBufAfterInFlight: pending(210) >= 128 → IO scheduled
        delegate.reset();
        tasksBefore = ioExecutor.submittedCount();
        writeTcfSync(writer, new byte[100]);
        assertEquals("one task submitted", tasksBefore + 1, ioExecutor.submittedCount());
        awaitAll();
        assertEquals(1, delegate.fileWriteCount);
        assertEquals(210, entry.cacheEndOffset);
        assertEquals(210, entry.writtenToFsOffset);

        // Write 100 bytes — pending(100) == writeSize(100), 100 < writeBatchBytes(128) → no IO
        delegate.reset();
        tasksBefore = ioExecutor.submittedCount();
        writeTcfSync(writer, new byte[100]);
        assertEquals("no task submitted", tasksBefore, ioExecutor.submittedCount());
        assertEquals(0, delegate.fileWriteCount);
        assertEquals(310, entry.cacheEndOffset);
        assertEquals(210, entry.writtenToFsOffset);

        byte[] onDisk = readFileSync(p);
        assertEquals(210, onDisk.length);

        tcf.close(writer).get();
    }

    @Test
    public void testReadAfterWriteAndFsync() throws Exception {
        String p = path("file5");
        AsyncFile writer = tcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        writeTcfSync(writer, new byte[]{1, 2, 3, 4, 5});
        // fsync flushes cache to disk
        delegate.reset();
        tcf.fsync(writer).get(5, TimeUnit.SECONDS);
        // fsync should trigger delegate write + fsync
        assertTrue("fsync should flush to delegate", delegate.fileWriteCount > 0 || delegate.fileFsyncCount > 0);
        // Data should be on disk
        assertArrayEquals(new byte[]{1, 2, 3, 4, 5}, readFileSync(p));
        // Separate reader reads
        delegate.reset();
        AsyncFile reader = tcf.open(p, AbstractStorageFile.OpenMode.READ, false, false, null).get();
        try {
            delegate.fileReadCount = 0;
            byte[] data = readTcfSync(reader, 5);
            assertArrayEquals(new byte[]{1, 2, 3, 4, 5}, data);
            assertEquals("read should hit cache", 0, delegate.fileReadCount);
        } finally {
            tcf.close(reader).get(5, TimeUnit.SECONDS);
        }
        tcf.close(writer).get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testWriteExceedsPerFileLimitKeepsFailing() throws Exception {
        String p = path("file6");
        // atomicReplace uses FULL_CACHE; payload larger than maxCacheSizePerFileBytes (10KB)
        AsyncFile file = tcf.open(p, AbstractStorageFile.OpenMode.WRITE, true, false, null).get();
        try {
            byte[] bigData = new byte[11 * 1024];
            Arrays.fill(bigData, (byte) 42);
            try {
                writeTcfSync(file, bigData);
                fail("expected CacheFileTooLargeException");
            } catch (Exception e) {
                assertTrue(e.getCause() instanceof CacheFileTooLargeException
                        || e instanceof CacheFileTooLargeException);
            }
            // No sticky no-cache fallback: the same oversized write keeps failing
            try {
                writeTcfSync(file, bigData);
                fail("expected CacheFileTooLargeException on retry");
            } catch (Exception e) {
                assertTrue(e.getCause() instanceof CacheFileTooLargeException
                        || e instanceof CacheFileTooLargeException);
            }
        } finally {
            tcf.close(file).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testNoCacheModePassesThrough() throws Exception {
        String p = path("file7");
        AsyncFile writer = tcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null,
                AbstractStorageFile.CacheMode.NO_CACHE).get();
        try {
            delegate.reset();
            writeTcfSync(writer, new byte[]{1, 2, 3});
            // NO_CACHE mode: write goes directly to delegate
            assertTrue("NO_CACHE write should go to delegate", delegate.fileWriteCount > 0);
            // No chunk memory should be allocated
            assertEquals(0, tcf.getGlobalCommittedBytes());
            // Data should be on disk
            tcf.fsync(writer).get(5, TimeUnit.SECONDS);
            assertArrayEquals(new byte[]{1, 2, 3}, readFileSync(p));
        } finally {
            tcf.close(writer).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testAtomicReplaceCache() throws Exception {
        String p = path("file8");
        writeFileSync(p, new byte[]{1, 2, 3});
        // Writer with atomicReplace replaces entire file content
        AsyncFile writer = tcf.open(p, AbstractStorageFile.OpenMode.WRITE, true, false, null).get();
        writeTcfSync(writer, new byte[]{10, 20, 30, 40});
        tcf.close(writer).get(5, TimeUnit.SECONDS);
        // Disk should have new content
        assertArrayEquals(new byte[]{10, 20, 30, 40}, readFileSync(p));
        // Separate reader reads new data
        AsyncFile reader = tcf.open(p, AbstractStorageFile.OpenMode.READ, false, false, null).get();
        try {
            byte[] data = readTcfSync(reader, 4);
            assertArrayEquals(new byte[]{10, 20, 30, 40}, data);
        } finally {
            tcf.close(reader).get(5, TimeUnit.SECONDS);
        }
    }

    // =========================================================================
    // B. AsyncFile metadata operations
    // =========================================================================

    @Test
    public void testSizeReflectsCache() throws Exception {
        String p = path("file9");
        writeFileSync(p, new byte[10]);
        // Writer appends 20 bytes
        AsyncFile writer = tcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        writeTcfSync(writer, new byte[20]);
        // Writer's size reflects cache (10 + 20 = 30) — no delegate size call needed
        delegate.reset();
        long writerSize = tcf.size(writer).get(5, TimeUnit.SECONDS);
        assertEquals(30, writerSize);
        assertEquals("size should come from cache, no delegate call", 0, delegate.fileReadCount);
        tcf.close(writer).get(5, TimeUnit.SECONDS);
        // Separate reader opens and verifies size = 30
        AsyncFile reader = tcf.open(p, AbstractStorageFile.OpenMode.READ, false, false, null).get();
        try {
            long readerSize = tcf.size(reader).get(5, TimeUnit.SECONDS);
            assertEquals(30, readerSize);
        } finally {
            tcf.close(reader).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testTruncateUpdatesCache() throws Exception {
        String p = path("file10");
        // Writer writes 200 bytes then truncates to 100
        AsyncFile writer = tcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        writeTcfSync(writer, new byte[200]);
        tcf.truncate(writer, 100).get(5, TimeUnit.SECONDS);
        // Writer's size reflects truncated cache
        long writerSize = tcf.size(writer).get(5, TimeUnit.SECONDS);
        assertEquals(100, writerSize);
        tcf.close(writer).get(5, TimeUnit.SECONDS);
        // Separate reader verifies size = 100
        AsyncFile reader = tcf.open(p, AbstractStorageFile.OpenMode.READ, false, false, null).get();
        try {
            long readerSize = tcf.size(reader).get(5, TimeUnit.SECONDS);
            assertEquals(100, readerSize);
        } finally {
            tcf.close(reader).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testFsyncFlushesCache() throws Exception {
        String p = path("file11");
        AsyncFile file = tcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        try {
            writeTcfSync(file, new byte[]{1, 2, 3, 4, 5});
            tcf.fsync(file).get(5, TimeUnit.SECONDS);
            // Data should be on disk after fsync
            assertArrayEquals(new byte[]{1, 2, 3, 4, 5}, readFileSync(p));
        } finally {
            tcf.close(file).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testDeleteClearsCacheAndFile() throws Exception {
        String p = path("file12");
        AsyncFile file = tcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        writeTcfSync(file, new byte[]{1, 2, 3});
        tcf.fsync(file).get(5, TimeUnit.SECONDS);
        assertTrue(Files.exists(Paths.get(p)));

        tcf.delete(file).get(5, TimeUnit.SECONDS);
        assertFalse(Files.exists(Paths.get(p)));
    }

    @Test
    public void testCloseOnClosedFileIsNoOp() throws Exception {
        String p = path("file14");
        AsyncFile file = tcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        tcf.close(file).get(5, TimeUnit.SECONDS);
        // Second close should not throw
        tcf.close(file).get(5, TimeUnit.SECONDS);
    }

    // =========================================================================
    // C. AsyncSegmentFile cache operations
    // =========================================================================

    private static final String SEG_PREFIX = "seg";
    private static final String IDX_PREFIX = "idx";
    private static final List<String> INDEX_PREFIXES = Collections.singletonList(IDX_PREFIX);

    @Test
    public void testSegmentWriteAndReadFromCache() throws Exception {
        String dir = path("segdir1");
        Files.createDirectories(Paths.get(dir));
        // Writer writes data
        AsyncSegmentFile writer = tcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        tcf.write(writer, bufOf(new byte[]{1, 2, 3, 4, 5})).get(5, TimeUnit.SECONDS);
        // Verify cache chunk has the written data
        FileCacheEntry writerEntry = writer.getCacheEntry();
        assertTrue(writerEntry.isInitialized());
        assertEquals(5, writerEntry.cacheEndOffset);
        tcf.close(writer).get(5, TimeUnit.SECONDS);
        // Separate reader reads (TAIL_CACHE reader reads flushed data from delegate)
        AsyncSegmentFile reader = tcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, false, null).get();
        try {
            ByteBuf buf = tcf.read(reader, 5).get(5, TimeUnit.SECONDS);
            assertArrayEquals(new byte[]{1, 2, 3, 4, 5}, readBytes(buf));
            // Verify reader cache entry is initialized
            assertTrue(reader.getCacheEntry().isInitialized());
        } finally {
            tcf.close(reader).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSegmentCloseThenReopen() throws Exception {
        String dir = path("segdir2");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile writer = tcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        tcf.write(writer, bufOf(new byte[]{10, 20, 30})).get(5, TimeUnit.SECONDS);
        delegate.reset();
        tcf.close(writer).get(5, TimeUnit.SECONDS);
        // Close flushes to delegate
        assertTrue("segment close should flush to delegate", delegate.segWriteCount > 0);

        // Reopen reader and verify data correctness
        AsyncSegmentFile reader = tcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, false, null).get();
        try {
            ByteBuf buf = tcf.read(reader, 3).get(5, TimeUnit.SECONDS);
            assertArrayEquals(new byte[]{10, 20, 30}, readBytes(buf));
        } finally {
            tcf.close(reader).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSegmentWriteThenRollThenRead() throws Exception {
        String dir = path("segdir3");
        Files.createDirectories(Paths.get(dir));
        // Writer: write, roll, write, close
        AsyncSegmentFile writer = tcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        tcf.write(writer, bufOf(new byte[]{1, 2, 3})).get(5, TimeUnit.SECONDS);
        tcf.roll(writer).get(5, TimeUnit.SECONDS);
        tcf.write(writer, bufOf(new byte[]{4, 5, 6})).get(5, TimeUnit.SECONDS);
        tcf.close(writer).get(5, TimeUnit.SECONDS);
        // Separate reader: read across both segments
        AsyncSegmentFile reader = tcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, false, null).get();
        try {
            ByteBuf buf1 = tcf.read(reader, 3).get(5, TimeUnit.SECONDS);
            assertArrayEquals(new byte[]{1, 2, 3}, readBytes(buf1));
            ByteBuf buf2 = tcf.read(reader, 3).get(5, TimeUnit.SECONDS);
            assertArrayEquals(new byte[]{4, 5, 6}, readBytes(buf2));
        } finally {
            tcf.close(reader).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSegmentSizeReflectsCache() throws Exception {
        String dir = path("segdir4");
        Files.createDirectories(Paths.get(dir));
        // Writer: write two segments
        AsyncSegmentFile writer = tcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        tcf.write(writer, bufOf(new byte[10])).get(5, TimeUnit.SECONDS);
        tcf.roll(writer).get(5, TimeUnit.SECONDS);
        tcf.write(writer, bufOf(new byte[20])).get(5, TimeUnit.SECONDS);
        // Writer's size reflects cache
        long writerSize = tcf.size(writer).get(5, TimeUnit.SECONDS);
        assertEquals(30, writerSize);
        tcf.close(writer).get(5, TimeUnit.SECONDS);
        // Separate reader verifies size
        AsyncSegmentFile reader = tcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, false, null).get();
        try {
            long readerSize = tcf.size(reader).get(5, TimeUnit.SECONDS);
            assertEquals(30, readerSize);
        } finally {
            tcf.close(reader).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSegmentTransferToFromCache() throws Exception {
        String dir = path("segdir7");
        Files.createDirectories(Paths.get(dir));
        // Writer writes data
        AsyncSegmentFile writer = tcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        tcf.write(writer, bufOf(new byte[]{10, 20, 30, 40, 50})).get(5, TimeUnit.SECONDS);
        tcf.close(writer).get(5, TimeUnit.SECONDS);
        // Separate reader transfers from cache
        AsyncSegmentFile reader = tcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, false, null).get();
        try {
            AsyncTFSBasedFileSystemTest.ByteArrayOutputStreamChannel target =
                    new AsyncTFSBasedFileSystemTest.ByteArrayOutputStreamChannel();
            long n = tcf.transferTo(reader, 1, 3, target).get(5, TimeUnit.SECONDS);
            assertEquals(3, n);
            assertArrayEquals(new byte[]{20, 30, 40}, target.toByteArray());
        } finally {
            tcf.close(reader).get(5, TimeUnit.SECONDS);
        }
    }

    /**
     * Reader opened on the live tail ({@code openedEnd=MAX}), then writer rolls and
     * appends a new segment. Distinguishes A (writer size stuck) vs B (reader
     * {@code transferTo} returns 0 at the roll boundary).
     */
    @Test
    public void testPreopenedReaderTransferToAfterWriterRoll() throws Exception {
        String dir = path("seg_preopened_roll");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile writer = tcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        tcf.write(writer, bufOf(new byte[64])).get(5, TimeUnit.SECONDS);

        AsyncSegmentFile reader = tcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, false, null).get();
        AsyncTFSBasedFileSystemTest.ByteArrayOutputStreamChannel first =
                new AsyncTFSBasedFileSystemTest.ByteArrayOutputStreamChannel();
        assertEquals(64, (long) tcf.transferTo(reader, 0, 64, first).get(5, TimeUnit.SECONDS));
        assertEquals(0, reader.openedSegmentStartOffset);
        assertEquals(Long.MAX_VALUE, reader.openedSegmentEndOffset);

        tcf.roll(writer).get(5, TimeUnit.SECONDS);
        tcf.write(writer, bufOf(new byte[200])).get(5, TimeUnit.SECONDS);

        long writerStart = tcf.getCurrentSegmentStartOffset(writer);
        long writerLastSize = tcf.sizeOfSegment(writer, writerStart).get(5, TimeUnit.SECONDS);
        long writerTotal = tcf.size(writer).get(5, TimeUnit.SECONDS);
        logger.info("[A] writerStart={} lastSize={} total={} list={}",
                writerStart, writerLastSize, writerTotal, tcf.list(writer));

        AsyncTFSBasedFileSystemTest.ByteArrayOutputStreamChannel second =
                new AsyncTFSBasedFileSystemTest.ByteArrayOutputStreamChannel();
        long n = tcf.transferTo(reader, 64, 200, second).get(5, TimeUnit.SECONDS);
        logger.info("[B] transferTo(64,200)={} readerOpened=[{}, {})",
                n, reader.openedSegmentStartOffset, reader.openedSegmentEndOffset);

        try {
            assertEquals("A: writer start after roll", 64L, writerStart);
            assertEquals("A: sizeOfSegment of new start should see 200B tail", 200L, writerLastSize);
            assertEquals("A: size() should include both segments", 264L, writerTotal);
            assertEquals("B: preopened reader transferTo at roll boundary", 200L, n);
            assertEquals(200, second.toByteArray().length);
        } finally {
            tcf.close(reader).get(5, TimeUnit.SECONDS);
            tcf.close(writer).get(5, TimeUnit.SECONDS);
        }
    }

    /**
     * Same as {@link #testPreopenedReaderTransferToAfterWriterRoll} but force the
     * disk {@code transferTo} path ({@code transferPreferCache=false}). This is B:
     * reader handle stays on {@code [oldStart, MAX)} after writer roll.
     */
    @Test
    public void testPreopenedReaderDiskTransferToAfterWriterRoll() throws Exception {
        TailCacheFileSystemConfig config = new TailCacheFileSystemConfig();
        config.setPerFileCacheLimits(10 * 1024, 1, CHUNK_SIZE);
        config.setMaxCacheSizeBytes(100 * 1024);
        config.setWriteBatchBytes(128);
        config.setIoWaitTimeoutMs(5000);
        config.setExpectedMinRetentionMs(0);
        config.setEvictScanIntervalMs(60_000);
        config.setWatermarkRatios(0.5, 0.8);
        config.setMaxEvictRatioPerWrite(0.5);
        config.setTransferPreferCache(false);
        TailCacheFileSystem tcfDisk = newTcf(config);

        String dir = path("seg_preopened_roll_disk");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile writer = tcfDisk.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        tcfDisk.write(writer, bufOf(new byte[64])).get(5, TimeUnit.SECONDS);
        tcfDisk.fsync(writer).get(5, TimeUnit.SECONDS);

        AsyncSegmentFile reader = tcfDisk.open(dir, SEG_PREFIX, INDEX_PREFIXES, false, null).get();
        AsyncTFSBasedFileSystemTest.ByteArrayOutputStreamChannel first =
                new AsyncTFSBasedFileSystemTest.ByteArrayOutputStreamChannel();
        assertEquals(64, (long) tcfDisk.transferTo(reader, 0, 64, first).get(5, TimeUnit.SECONDS));
        assertEquals(Long.MAX_VALUE, reader.openedSegmentEndOffset);

        tcfDisk.roll(writer).get(5, TimeUnit.SECONDS);
        tcfDisk.write(writer, bufOf(new byte[200])).get(5, TimeUnit.SECONDS);
        tcfDisk.fsync(writer).get(5, TimeUnit.SECONDS);

        AsyncTFSBasedFileSystemTest.ByteArrayOutputStreamChannel second =
                new AsyncTFSBasedFileSystemTest.ByteArrayOutputStreamChannel();
        long firstCall = tcfDisk.transferTo(reader, 64, 200, second).get(5, TimeUnit.SECONDS);
        logger.info("[B-disk] first transferTo(64,200)={} readerOpened=[{}, {})",
                firstCall, reader.openedSegmentStartOffset, reader.openedSegmentEndOffset);
        // First disk transferTo hits the sealed old tail file and returns 0. maybeSwitchSegment
        // only drops the channel — it does not rebind the opened range, so the reader still
        // reports the stale [0, MAX).
        assertEquals("B-disk first call is 0 at the sealed old tail", 0L, firstCall);
        assertEquals(0L, reader.openedSegmentStartOffset);
        assertEquals(Long.MAX_VALUE, reader.openedSegmentEndOffset);
        assertNull("channel dropped so the next call re-switches", reader.currentSegmentChannel);

        // The rebind happens on the next call: preReadMetadata sees isSegmentReady(64)==false
        // (channel is null) and switchToSegment moves the range to [64, MAX).
        long secondCall = tcfDisk.transferTo(reader, 64, 200, second).get(5, TimeUnit.SECONDS);
        logger.info("[B-disk] second transferTo(64,200)={} readerOpened=[{}, {})",
                secondCall, reader.openedSegmentStartOffset, reader.openedSegmentEndOffset);

        try {
            assertEquals("B-disk: retry after the channel drop must read the new segment", 200L, secondCall);
            assertEquals(64L, reader.openedSegmentStartOffset);
            assertEquals(Long.MAX_VALUE, reader.openedSegmentEndOffset);
        } finally {
            tcfDisk.close(reader).get(5, TimeUnit.SECONDS);
            tcfDisk.close(writer).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSegmentDeleteAllClearsCache() throws Exception {
        String dir = path("segdir8");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = tcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        tcf.write(seg, bufOf(new byte[10])).get(5, TimeUnit.SECONDS);
        tcf.roll(seg).get(5, TimeUnit.SECONDS);
        tcf.write(seg, bufOf(new byte[20])).get(5, TimeUnit.SECONDS);

        tcf.delete(seg).get(5, TimeUnit.SECONDS);
        File[] remaining = new File(dir).listFiles();
        if (remaining != null) {
            for (File f : remaining) {
                assertFalse("segment file should be deleted: " + f.getName(), f.getName().startsWith(SEG_PREFIX));
                assertFalse("index file should be deleted: " + f.getName(), f.getName().startsWith(IDX_PREFIX));
            }
        }
    }

    // =========================================================================
    // D. Eviction & memory management
    // =========================================================================

    /**
     * Config for the eviction-policy tests. Eviction is deterministic once three things hold:
     * writes never auto-flush (writeBatchBytes far above the write size, so writtenToFsOffset only
     * moves on an explicit fsync), allowDirtyEvict is false (true only under NO_FS/fsInconsistent),
     * and evictTailBeforeAppend runs *before* the append, so maxEvictable is
     * (chunksBeforeAppend - minRetainChunks).
     */
    private TailCacheFileSystemConfig evictConfig(int minRetainChunks, long maxCacheSizeBytes,
            long expectedMinRetentionMs) {
        TailCacheFileSystemConfig config = new TailCacheFileSystemConfig();
        config.setPerFileCacheLimits(10 * 1024, minRetainChunks, CHUNK_SIZE);
        config.setMaxCacheSizeBytes(maxCacheSizeBytes);
        config.setWriteBatchBytes(1024);
        config.setIoWaitTimeoutMs(5000);
        config.setExpectedMinRetentionMs(expectedMinRetentionMs);
        config.setEvictScanIntervalMs(60_000);
        config.setWatermarkRatios(0.3, 0.5);
        config.setMaxEvictRatioPerWrite(0.5);
        return config;
    }

    private void writeChunkAndFsync(TailCacheFileSystem fs, AsyncFile file) throws Exception {
        fs.write(file, bufOf(new byte[(int) CHUNK_SIZE])).get(5, TimeUnit.SECONDS);
        fs.fsync(file).get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testEvictionRetainsMinChunksPlusAppended() throws Exception {
        // minRetainChunks=2 with retention=0 and every chunk durable: each write evicts exactly
        // maxEvictable = (size - 2) chunks and then appends 1, so the cache settles at 3 chunks.
        // Compare with testCacheStartOffsetAfterEviction, which uses minRetainChunks=1 and
        // settles at 2 — that difference is what pins minRetainChunks down.
        TailCacheFileSystem tightTcf = newTcf(evictConfig(2, 200, 0));
        String p = path("file_evict_min_retain");
        AsyncFile file = tightTcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        FileCacheEntry entry = file.getCacheEntry();

        // Chunks 0..2: maxEvictable is 0/0/0, so nothing is evicted yet.
        for (int i = 0; i < 3; i++) {
            writeChunkAndFsync(tightTcf, file);
        }
        assertEquals(3, entry.chunks.size());
        assertEquals(0, entry.cacheStartOffset);

        // Chunk 3: maxEvictable = 3-2 = 1 → evicts chunk 0 only.
        writeChunkAndFsync(tightTcf, file);
        assertNull("chunk 0 evicted", entry.chunks.get(0L));
        assertEquals(3, entry.chunks.size());
        assertEquals(CHUNK_SIZE, entry.cacheStartOffset);

        // Chunk 4: steady state — evict exactly one, append exactly one.
        writeChunkAndFsync(tightTcf, file);
        assertNull("chunk 1 evicted", entry.chunks.get(1L));
        assertNotNull(entry.chunks.get(2L));
        assertNotNull(entry.chunks.get(3L));
        assertNotNull(entry.chunks.get(4L));
        assertEquals("settles at minRetainChunks + the freshly appended chunk", 3, entry.chunks.size());
        assertEquals(2 * CHUNK_SIZE, entry.cacheStartOffset);
        assertEquals(3 * CHUNK_SIZE, entry.bodySizeBytes);

        // Everything evicted was already flushed, so it is still readable from disk.
        assertEquals(5 * CHUNK_SIZE, readFileSync(p).length);
        tightTcf.close(file).get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testRetentionBlocksEvictionBelowLowWatermark() throws Exception {
        // ratio < lowWatermark → decideEvictionPolicy returns minEvict=0, so the only thing that
        // can evict is chunk expiry. With a 60s retention nothing has expired, so no chunk is
        // dropped even though all of them are durable and maxEvictable > 0.
        TailCacheFileSystem looseTcf = newTcf(evictConfig(1, 100 * 1024, 60_000));
        String p = path("file_evict_retention");
        AsyncFile file = looseTcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        FileCacheEntry entry = file.getCacheEntry();

        for (int i = 0; i < 5; i++) {
            writeChunkAndFsync(looseTcf, file);
        }
        assertEquals("retention keeps every chunk", 5, entry.chunks.size());
        assertEquals(0, entry.cacheStartOffset);
        assertEquals(5 * CHUNK_SIZE, entry.bodySizeBytes);
        looseTcf.close(file).get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testZeroRetentionEvictsBelowLowWatermark() throws Exception {
        // Same watermark position as testRetentionBlocksEvictionBelowLowWatermark, retention=0.
        // Now every durable chunk is immediately expired, so the first while loop evicts up to
        // maxEvictable on every write and the cache settles at minRetainChunks + 1.
        TailCacheFileSystem looseTcf = newTcf(evictConfig(1, 100 * 1024, 0));
        String p = path("file_evict_no_retention");
        AsyncFile file = looseTcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        FileCacheEntry entry = file.getCacheEntry();

        for (int i = 0; i < 5; i++) {
            writeChunkAndFsync(looseTcf, file);
        }
        assertEquals("expired chunks are evicted regardless of watermark", 2, entry.chunks.size());
        assertNotNull(entry.chunks.get(3L));
        assertNotNull(entry.chunks.get(4L));
        assertEquals(3 * CHUNK_SIZE, entry.cacheStartOffset);
        looseTcf.close(file).get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testHighWatermarkEvictsDespiteRetention() throws Exception {
        // ratio >= highWatermark → minEvict = round(maxEvictable * maxEvictRatioPerWrite), and the
        // second while loop honours it without consulting chunk expiry. So a 60s retention that
        // blocks eviction entirely below the low watermark no longer does here.
        TailCacheFileSystem tightTcf = newTcf(evictConfig(1, 200, 60_000));
        String p = path("file_evict_high_wm");
        AsyncFile file = tightTcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        FileCacheEntry entry = file.getCacheEntry();

        // Chunks 0..1: maxEvictable is 0 then 0, nothing evicted.
        writeChunkAndFsync(tightTcf, file);
        writeChunkAndFsync(tightTcf, file);
        assertEquals(2, entry.chunks.size());

        // Chunk 2: committed(128)/200 = 0.64 >= high(0.5) → minEvict = round(1 * 0.5) = 1.
        // Retention has not expired, so this eviction comes purely from the minEvict loop.
        writeChunkAndFsync(tightTcf, file);
        assertNull("high watermark evicts an unexpired chunk", entry.chunks.get(0L));
        assertEquals(2, entry.chunks.size());
        assertEquals(CHUNK_SIZE, entry.cacheStartOffset);
        tightTcf.close(file).get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testUndurableChunksAreNotEvicted() throws Exception {
        // Same tight config as testCacheStartOffsetAfterEviction but without fsync, so
        // durableFsOffset stays 0 and the first while loop hits durableLimit on chunk 0 and bails.
        // The minEvict loop is skipped entirely when durableLimit is set, so nothing is dropped.
        // Kept at 3 chunks (192B) so the reserve stays under maxCacheSizeBytes(200) — a 4th chunk
        // would block for ioWaitTimeoutMs and fail with CacheMemoryReserveException instead.
        TailCacheFileSystem tightTcf = newTcf(evictConfig(1, 200, 0));
        String p = path("file_evict_undurable");
        AsyncFile file = tightTcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        FileCacheEntry entry = file.getCacheEntry();

        for (int i = 0; i < 3; i++) {
            tightTcf.write(file, bufOf(new byte[(int) CHUNK_SIZE])).get(5, TimeUnit.SECONDS);
        }
        // Precondition for the branch under test: nothing has reached the backing FS.
        assertEquals(0, entry.writtenToFsOffset);
        assertEquals(0, entry.pendingFsyncBytes);

        assertNotNull("undurable chunk 0 must survive", entry.chunks.get(0L));
        assertEquals(3, entry.chunks.size());
        assertEquals(0, entry.cacheStartOffset);
        assertEquals(3 * CHUNK_SIZE, entry.bodySizeBytes);
        assertFalse("nothing durable, so the cache must not be marked inconsistent", entry.fsInconsistent);
        tightTcf.close(file).get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testMemoryTrackerTracking() throws Exception {
        String p = path("file18");
        long before = tcf.getGlobalCommittedBytes();

        AsyncFile file = tcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        writeTcfSync(file, new byte[(int) CHUNK_SIZE]);

        // Memory should increase after write
        long afterWrite = tcf.getGlobalCommittedBytes();
        assertTrue("committed bytes should increase after write", afterWrite > before);

        // Close should release memory
        tcf.close(file).get(5, TimeUnit.SECONDS);
        long afterClose = tcf.getGlobalCommittedBytes();
        assertTrue("committed bytes should decrease after close", afterClose < afterWrite);
    }

    // =========================================================================
    // E. BackingFsMode.NO_FS — the disk is assumed unreachable (hung), so nothing may touch it.
    // Local files are only a buffer between upstream and downstream; losing them, or having them
    // overwritten by a freshly opened writer, is an accepted trade-off for staying available.
    // =========================================================================

    @Test
    public void testNoFsRejectsNoCacheMode() throws Exception {
        // Without a cache there is nowhere for data to live under NO_FS, so the combination is
        // rejected at open time rather than failing later on the first write.
        TailCacheFileSystem noFsTcf = newNoFsTcf();
        String p = path("nofs_nocache");
        try {
            noFsTcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null,
                    AbstractStorageFile.CacheMode.NO_CACHE);
            fail("expected IllegalArgumentException for NO_FS + NO_CACHE");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("NO_CACHE"));
        }

        String dir = path("nofs_nocache_seg");
        Files.createDirectories(Paths.get(dir));
        try {
            noFsTcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null,
                    AbstractStorageFile.CacheMode.NO_CACHE);
            fail("expected IllegalArgumentException for NO_FS + NO_CACHE segment");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("NO_CACHE"));
        }
    }

    @Test
    public void testNoFsOpenInitializesCacheAtZeroAndMarksInconsistent() throws Exception {
        // NO_FS open skips initFromDisk entirely, so the cache cannot be seeded from the file.
        // initStorageCache instead seeds it at offset 0 and marks it inconsistent up front —
        // the cache is "initialized" (isInitialized() is true) but empty and known to disagree
        // with whatever is on disk.
        TailCacheFileSystem noFsTcf = newNoFsTcf();
        String p = path("nofs_open_state");
        writeFileSync(p, new byte[]{9, 9, 9, 9});
        delegate.reset();

        AsyncFile writer = noFsTcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        try {
            FileCacheEntry entry = writer.getCacheEntry();
            assertTrue(entry.isInitialized());
            assertEquals(0, entry.cacheStartOffset);
            assertEquals(0, entry.cacheEndOffset);
            assertEquals(0, entry.writtenToFsOffset);
            assertTrue("NO_FS open marks the cache inconsistent with disk", entry.fsInconsistent);
            assertTrue("channel is not opened under NO_FS", writer.needPrepare);
            assertEquals("open must not touch the disk", 0, delegate.fileReadCount);
            // The existing on-disk content is untouched and invisible.
            assertArrayEquals(new byte[]{9, 9, 9, 9}, readFileSync(p));
        } finally {
            noFsTcf.close(writer).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testNoFsWriteAndReadStayInMemory() throws Exception {
        // Writes never reach the delegate and never leave the cache.
        TailCacheFileSystem noFsTcf = newNoFsTcf();
        String p = path("nofs_write_mem");
        AsyncFile writer = noFsTcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        try {
            delegate.reset();
            // Well above writeBatchBytes(128) — under ASYNC this would have triggered a flush.
            noFsTcf.write(writer, bufOf(new byte[200])).get(5, TimeUnit.SECONDS);
            awaitAll();

            FileCacheEntry entry = writer.getCacheEntry();
            assertEquals(200, entry.cacheEndOffset);
            assertEquals("nothing was handed to the delegate", 0, entry.writtenToFsOffset);
            assertEquals(0, delegate.fileWriteCount);
            assertFalse("the file must not be created", Files.exists(Paths.get(p)));

            // Readable straight from the cache.
            byte[] data = readBytes(noFsTcf.read(writer, 200, 0).get(5, TimeUnit.SECONDS));
            assertEquals(200, data.length);
            assertEquals(0, delegate.fileReadCount);
        } finally {
            noFsTcf.close(writer).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testNoFsFsyncIsNoOp() throws Exception {
        TailCacheFileSystem noFsTcf = newNoFsTcf();
        String p = path("nofs_fsync_noop");
        AsyncFile writer = noFsTcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        try {
            noFsTcf.write(writer, bufOf(new byte[]{1, 2, 3})).get(5, TimeUnit.SECONDS);
            delegate.reset();
            noFsTcf.fsync(writer).get(5, TimeUnit.SECONDS);
            awaitAll();

            FileCacheEntry entry = writer.getCacheEntry();
            assertEquals(0, delegate.fileWriteCount);
            assertEquals(0, delegate.fileFsyncCount);
            assertEquals("fsync must not advance the flushed offset", 0, entry.writtenToFsOffset);
            assertEquals(3, entry.cacheEndOffset);
            assertFalse(Files.exists(Paths.get(p)));
        } finally {
            noFsTcf.close(writer).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testNoFsReadBelowCacheStartThrows() throws Exception {
        // A reader opened under ASYNC has cacheStartOffset == fileSize (tail cache holds no history).
        // After switching to NO_FS the disk is off limits, so offsets below the cache window become
        // unreadable rather than silently degrading to a disk read.
        TailCacheFileSystem tcfSwitch = newTcf(baseConfig());
        String p = path("nofs_read_below_start");
        AsyncFile writer = tcfSwitch.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        tcfSwitch.write(writer, bufOf(new byte[]{1, 2, 3, 4, 5})).get(5, TimeUnit.SECONDS);
        tcfSwitch.fsync(writer).get(5, TimeUnit.SECONDS);
        tcfSwitch.close(writer).get(5, TimeUnit.SECONDS);

        AsyncFile reader = tcfSwitch.open(p, AbstractStorageFile.OpenMode.READ, false, false, null).get();
        try {
            FileCacheEntry entry = reader.getCacheEntry();
            assertEquals("reader cache window starts at EOF", 5, entry.cacheStartOffset);
            // Still fine under ASYNC — degrades to the disk.
            assertArrayEquals(new byte[]{1, 2, 3},
                    readBytes(tcfSwitch.read(reader, 3, 0).get(5, TimeUnit.SECONDS)));

            tcfSwitch.setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.NO_FS);
            try {
                tcfSwitch.read(reader, 3, 0);
                fail("expected CannotReadPositionInNoFsException");
            } catch (CannotReadPositionInNoFsException expected) {
                // offset 0 is below cacheStartOffset and the disk is unreachable
            }
            // transferTo takes the same decision path.
            try {
                tcfSwitch.transferTo(reader, 0, 3,
                        new AsyncTFSBasedFileSystemTest.ByteArrayOutputStreamChannel());
                fail("expected CannotReadPositionInNoFsException");
            } catch (CannotReadPositionInNoFsException expected) {
                // same
            }
        } finally {
            tcfSwitch.setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.ASYNC);
            tcfSwitch.close(reader).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testNoFsReadInsideCacheWindowSucceeds() throws Exception {
        // Counterpart to the previous test: inside the window preferCacheRead returns
        // (true, false) — serve from cache, never degrade — so the read still works.
        TailCacheFileSystem noFsTcf = newNoFsTcf();
        String p = path("nofs_read_in_window");
        AsyncFile writer = noFsTcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        try {
            noFsTcf.write(writer, bufOf(new byte[]{1, 2, 3, 4, 5})).get(5, TimeUnit.SECONDS);
            assertArrayEquals(new byte[]{3, 4, 5},
                    readBytes(noFsTcf.read(writer, 3, 2).get(5, TimeUnit.SECONDS)));

            // Beyond cacheEndOffset is an empty read, not an error.
            assertEquals(0, readBytes(noFsTcf.read(writer, 3, 5).get(5, TimeUnit.SECONDS)).length);

            AsyncTFSBasedFileSystemTest.ByteArrayOutputStreamChannel target =
                    new AsyncTFSBasedFileSystemTest.ByteArrayOutputStreamChannel();
            assertEquals(3L, (long) noFsTcf.transferTo(writer, 1, 3, target).get(5, TimeUnit.SECONDS));
            assertArrayEquals(new byte[]{2, 3, 4}, target.toByteArray());
        } finally {
            noFsTcf.close(writer).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testNoFsDirtyEvictionDropsDataAndKeepsInconsistent() throws Exception {
        // The one place NO_FS silently loses data: allowDirtyEvict is (noFs || fsInconsistent), so
        // under memory pressure evictTailBeforeAppend will drop chunks that were never flushed
        // instead of refusing to evict. cacheStartOffset moves past them for good.
        TailCacheFileSystem tightTcf = newTcf(
                evictConfig(1, 200, 0).setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.NO_FS));
        String p = path("nofs_dirty_evict");
        AsyncFile writer = tightTcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        try {
            FileCacheEntry entry = writer.getCacheEntry();
            for (int i = 0; i < 3; i++) {
                tightTcf.write(writer, bufOf(new byte[(int) CHUNK_SIZE])).get(5, TimeUnit.SECONDS);
            }
            // Nothing is durable (writtenToFsOffset stays 0), yet chunk 0 is gone: under ASYNC the
            // durableLimit check would have kept it (see testUndurableChunksAreNotEvicted).
            assertEquals(0, entry.writtenToFsOffset);
            assertNull("undurable chunk evicted under NO_FS", entry.chunks.get(0L));
            assertEquals(CHUNK_SIZE, entry.cacheStartOffset);
            assertTrue(entry.fsInconsistent);

            // The dropped range is now unreachable: no cache, no disk.
            try {
                tightTcf.read(writer, 8, 0);
                fail("expected CannotReadPositionInNoFsException for the evicted range");
            } catch (CannotReadPositionInNoFsException expected) {
                // data is gone
            }
        } finally {
            tightTcf.close(writer).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testNoFsDirectoryAndExistenceQueriesThrow() throws Exception {
        // These cannot be answered without touching the disk, and answering "false" would be a lie
        // that callers would act on, so they fail loudly instead.
        TailCacheFileSystem noFsTcf = newNoFsTcf();
        String p = path("nofs_queries");
        try {
            noFsTcf.exists(p);
            fail("expected CannotDetermineInNoFsException from exists");
        } catch (CannotDetermineInNoFsException expected) {
            // expected
        }
        try {
            noFsTcf.isDirectory(p);
            fail("expected CannotDetermineInNoFsException from isDirectory");
        } catch (CannotDetermineInNoFsException expected) {
            // expected
        }
        try {
            noFsTcf.list(p);
            fail("expected CannotDetermineInNoFsException from list");
        } catch (CannotDetermineInNoFsException expected) {
            // expected
        }
    }

    @Test
    public void testNoFsMutationsOnPathsAreSilentlySkipped() throws Exception {
        // mkdir/rmdir/delete(path) have no cache counterpart, so instead of failing they report
        // success and do nothing — callers treat them as best-effort housekeeping.
        TailCacheFileSystem noFsTcf = newNoFsTcf();
        String dir = path("nofs_mkdir");
        String p = path("nofs_delete_path");
        writeFileSync(p, new byte[]{1});

        assertTrue(noFsTcf.mkdir(dir, true).get(5, TimeUnit.SECONDS));
        assertFalse("mkdir must not create anything", Files.exists(Paths.get(dir)));
        assertTrue(noFsTcf.rmdir(dir, true).get(5, TimeUnit.SECONDS));

        noFsTcf.delete(p).get(5, TimeUnit.SECONDS);
        assertTrue("delete(path) must leave the file alone", Files.exists(Paths.get(p)));
    }

    @Test
    public void testNoFsSizeReportsLogicalLengthNotReadableRange() throws Exception {
        // size() answers from the cache, so it keeps working. Note it reports total bytes written,
        // which after a dirty eviction is *not* the readable range — the readable lower bound is
        // cacheStartOffset. Callers of a tail cache are expected to know that.
        TailCacheFileSystem noFsTcf = newNoFsTcf();
        String p = path("nofs_size");
        AsyncFile writer = noFsTcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        try {
            noFsTcf.write(writer, bufOf(new byte[70])).get(5, TimeUnit.SECONDS);
            assertEquals(70, (long) noFsTcf.size(writer).get(5, TimeUnit.SECONDS));
            // lastModified falls back to the in-memory timestamp instead of stat()ing the file.
            assertTrue(noFsTcf.lastModified(writer).get(5, TimeUnit.SECONDS) > 0);
        } finally {
            noFsTcf.close(writer).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testNoFsCloseDropsUnflushedData() throws Exception {
        // NO_FS close skips the flush entirely and then releases the cache entry, so a writer's
        // data is gone once the last holder closes. This is the accepted trade-off.
        TailCacheFileSystem noFsTcf = newNoFsTcf();
        String p = path("nofs_close_drop");
        writeFileSync(p, new byte[]{7, 7});
        AsyncFile writer = noFsTcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        noFsTcf.write(writer, bufOf(new byte[]{1, 2, 3})).get(5, TimeUnit.SECONDS);
        delegate.reset();

        noFsTcf.close(writer).get(5, TimeUnit.SECONDS);
        awaitAll();
        assertEquals("close must not flush under NO_FS", 0, delegate.fileWriteCount);
        assertEquals(0, delegate.fileFsyncCount);
        assertArrayEquals("the pre-existing file is untouched", new byte[]{7, 7}, readFileSync(p));
        assertEquals("cache memory released", 0, noFsTcf.getGlobalCommittedBytes());
    }

    // ---- NO_FS segment metadata ----

    @Test
    public void testNoFsSegmentOpenIgnoresExistingSegments() throws Exception {
        // initFromDisk is skipped, so openInitialResources sees an empty state and creates segment 0
        // from scratch. The writer therefore starts at offset 0 and the existing segments stay
        // invisible on disk. This is intentional: with a hung disk there is no way to read them, and
        // overwriting the local buffer is acceptable for a proxy whose source of truth is upstream.
        String dir = path("nofs_seg_ignore_existing");
        Files.createDirectories(Paths.get(dir));
        Files.write(Paths.get(dir, SEG_PREFIX + "0"), new byte[64]);
        Files.write(Paths.get(dir, SEG_PREFIX + "64"), new byte[64]);

        TailCacheFileSystem noFsTcf = newNoFsTcf();
        AsyncSegmentFile writer = noFsTcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        try {
            assertEquals("state is built from nothing, not from disk",
                    Collections.singletonList(0L), noFsTcf.list(writer));
            assertEquals(0, writer.openedSegmentStartOffset);
            // Both files are still there; NO_FS never unlinks anything.
            assertTrue(Files.exists(Paths.get(dir, SEG_PREFIX + "0")));
            assertTrue(Files.exists(Paths.get(dir, SEG_PREFIX + "64")));
        } finally {
            noFsTcf.close(writer).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testNoFsSegmentRollAdvancesMetadataOnly() throws Exception {
        TailCacheFileSystem noFsTcf = newNoFsTcf();
        String dir = path("nofs_seg_roll");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile writer = noFsTcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        try {
            noFsTcf.write(writer, bufOf(new byte[10])).get(5, TimeUnit.SECONDS);
            delegate.reset();
            noFsTcf.roll(writer).get(5, TimeUnit.SECONDS);
            awaitAll();

            assertEquals(Arrays.asList(0L, 10L), noFsTcf.list(writer));
            assertEquals(10, writer.openedSegmentStartOffset);
            assertTrue(writer.getCacheEntry().fsInconsistent);
            assertEquals("roll must not write to the delegate", 0, delegate.segWriteCount);
            assertFalse("no segment file is created", Files.exists(Paths.get(dir, SEG_PREFIX + "0")));
            assertFalse(Files.exists(Paths.get(dir, SEG_PREFIX + "10")));

            // Data written after the roll still lands in the same cache and is readable.
            noFsTcf.write(writer, bufOf(new byte[]{4, 5, 6})).get(5, TimeUnit.SECONDS);
            assertArrayEquals(new byte[]{4, 5, 6},
                    readBytes(noFsTcf.read(writer, 3, 10).get(5, TimeUnit.SECONDS)));
        } finally {
            noFsTcf.close(writer).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testNoFsSegmentTruncateAndDeleteTouchMetadataOnly() throws Exception {
        TailCacheFileSystem noFsTcf = newNoFsTcf();
        String dir = path("nofs_seg_trunc_del");
        Files.createDirectories(Paths.get(dir));
        // Pre-existing files that NO_FS never sees and never removes.
        Files.write(Paths.get(dir, SEG_PREFIX + "0"), new byte[8]);

        AsyncSegmentFile writer = noFsTcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        try {
            noFsTcf.write(writer, bufOf(new byte[30])).get(5, TimeUnit.SECONDS);
            noFsTcf.roll(writer).get(5, TimeUnit.SECONDS);
            noFsTcf.write(writer, bufOf(new byte[20])).get(5, TimeUnit.SECONDS);
            assertEquals(Arrays.asList(0L, 30L), noFsTcf.list(writer));

            noFsTcf.truncate(writer, 40).get(5, TimeUnit.SECONDS);
            assertEquals(40, writer.getCacheEntry().cacheEndOffset);
            assertTrue(writer.getCacheEntry().fsInconsistent);

            // deleteSegments only drops metadata; the on-disk file survives as an orphan for a
            // later restore to clean up via deleteOrphanSegmentFilesSync.
            noFsTcf.deleteSegments(writer, Collections.singletonList(0L)).get(5, TimeUnit.SECONDS);
            awaitAll();
            assertEquals(Collections.singletonList(30L), noFsTcf.list(writer));
            assertTrue("orphan file left on disk", Files.exists(Paths.get(dir, SEG_PREFIX + "0")));
            assertEquals(8, Files.size(Paths.get(dir, SEG_PREFIX + "0")));
        } finally {
            noFsTcf.close(writer).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testNoFsSegmentDeleteAllKeepsFilesOnDisk() throws Exception {
        TailCacheFileSystem noFsTcf = newNoFsTcf();
        String dir = path("nofs_seg_delete_all");
        Files.createDirectories(Paths.get(dir));
        Files.write(Paths.get(dir, SEG_PREFIX + "0"), new byte[16]);

        AsyncSegmentFile writer = noFsTcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        try {
            noFsTcf.write(writer, bufOf(new byte[20])).get(5, TimeUnit.SECONDS);
            delegate.reset();
            noFsTcf.delete(writer).get(5, TimeUnit.SECONDS);
            awaitAll();

            assertTrue(noFsTcf.list(writer).isEmpty());
            FileCacheEntry entry = writer.getCacheEntry();
            assertEquals(0, entry.cacheEndOffset);
            assertTrue(entry.fsInconsistent);
            assertTrue("delete must not unlink under NO_FS",
                    Files.exists(Paths.get(dir, SEG_PREFIX + "0")));
        } finally {
            noFsTcf.close(writer).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testNoFsSegmentWriterOperationsRequireInitializedCache() throws Exception {
        // A NO_FS writer whose cache entry is not initialized has nowhere to put anything, so the
        // metadata-mutating operations refuse rather than corrupt state. Reaching that state needs
        // the entry to be shared with an already-open handle whose init was skipped, so drive the
        // check through a NO_CACHE segment writer opened under ASYNC and then switched to NO_FS.
        TailCacheFileSystem tcfSwitch = newTcf(baseConfig());
        String dir = path("nofs_seg_no_cache_entry");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile writer = tcfSwitch.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null,
                AbstractStorageFile.CacheMode.NO_CACHE).get();
        try {
            assertNull("NO_CACHE segment has no cache entry", writer.getCacheEntry());
            tcfSwitch.setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.NO_FS);

            try {
                tcfSwitch.roll(writer);
                fail("expected CannotWriteWithoutCacheInNoFsException from roll");
            } catch (CannotWriteWithoutCacheInNoFsException expected) {
                // expected
            }
            try {
                tcfSwitch.truncate(writer, 0);
                fail("expected CannotWriteWithoutCacheInNoFsException from truncate");
            } catch (CannotWriteWithoutCacheInNoFsException expected) {
                // expected
            }
            try {
                tcfSwitch.delete(writer);
                fail("expected CannotWriteWithoutCacheInNoFsException from delete");
            } catch (CannotWriteWithoutCacheInNoFsException expected) {
                // expected
            }
            try {
                tcfSwitch.write(writer, bufOf(new byte[]{1}));
                fail("expected CannotWriteWithoutCacheInNoFsException from write");
            } catch (CannotWriteWithoutCacheInNoFsException expected) {
                // write without a cache has nowhere to buffer
            }
        } finally {
            tcfSwitch.setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.ASYNC);
            tcfSwitch.close(writer).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testNoFsSegmentSizeAndLastModifiedOfSegment() throws Exception {
        TailCacheFileSystem noFsTcf = newNoFsTcf();
        String dir = path("nofs_seg_size");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile writer = noFsTcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        try {
            noFsTcf.write(writer, bufOf(new byte[10])).get(5, TimeUnit.SECONDS);
            noFsTcf.roll(writer).get(5, TimeUnit.SECONDS);
            noFsTcf.write(writer, bufOf(new byte[25])).get(5, TimeUnit.SECONDS);

            assertEquals(35, (long) noFsTcf.size(writer).get(5, TimeUnit.SECONDS));
            // Only the tail segment can be answered from the cache.
            assertTrue(noFsTcf.lastModifiedOfSegment(writer, 10).get(5, TimeUnit.SECONDS) > 0);
            try {
                noFsTcf.lastModifiedOfSegment(writer, 0);
                fail("expected CannotDetermineInNoFsException for a non-tail segment");
            } catch (CannotDetermineInNoFsException expected) {
                // only the tail segment is backed by the cache
            }
        } finally {
            noFsTcf.close(writer).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testNoFsGetCurrentIndexFilesReturnsCachedHandles() throws Exception {
        // Index handles are still handed out under NO_FS (writers must be able to append index
        // entries into the cache), but nothing is created on disk.
        TailCacheFileSystem noFsTcf = newNoFsTcf();
        String dir = path("nofs_seg_index");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile writer = noFsTcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        try {
            noFsTcf.write(writer, bufOf(new byte[10])).get(5, TimeUnit.SECONDS);
            Pair<Long, Map<String, AsyncFile>> result =
                    noFsTcf.getCurrentIndexFiles(writer, INDEX_PREFIXES).get(5, TimeUnit.SECONDS);
            assertEquals(Long.valueOf(0), result.getKey());
            AsyncFile idx = result.getValue().get(IDX_PREFIX);
            assertNotNull(idx);
            assertTrue("index channel stays unopened under NO_FS", idx.needPrepare);
            assertFalse(Files.exists(Paths.get(dir, IDX_PREFIX + "0")));
        } finally {
            noFsTcf.close(writer).get(5, TimeUnit.SECONDS);
        }
    }

    // =========================================================================
    // E2. Wedged disk under ASYNC — the scenario NO_FS exists for. Every entry point must come back
    // within its configured budget instead of inheriting the disk's hang.
    // =========================================================================

    // Generous relative to ioWaitTimeoutMs(200) below, but far below the 30s fault gate: crossing
    // it means the call actually waited on the disk.
    private static final long BOUNDED_MS = 3000;

    private TailCacheFileSystemConfig hangConfig() {
        return baseConfig().setIoWaitTimeoutMs(200).setRestoreWaitTimeoutMs(200);
    }

    @Test
    public void testHungDiskWriteReturnsBoundedAndKeepsDataInCache() throws Exception {
        FaultyDelegate faulty = newFaultyDelegate();
        TailCacheFileSystem hangTcf = newTcf(faulty, hangConfig());
        String p = path("hang_write");
        AsyncFile writer = hangTcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        FileCacheEntry entry = writer.getCacheEntry();

        // First write exceeds writeBatchBytes(128) so it submits IO, which then wedges.
        faulty.hangOn(Op.FILE_WRITE);
        long start = System.nanoTime();
        for (int i = 0; i < 5; i++) {
            hangTcf.write(writer, bufOf(new byte[200])).get(5, TimeUnit.SECONDS);
        }
        long elapsedMs = (System.nanoTime() - start) / 1_000_000L;

        assertTrue("writes must not inherit the disk hang, took " + elapsedMs + "ms",
                elapsedMs < BOUNDED_MS);
        assertEquals("all data accepted into the cache", 1000, entry.cacheEndOffset);
        // Readable from cache while the disk is still wedged.
        assertEquals(1000, readBytes(hangTcf.read(writer, 1000, 0).get(5, TimeUnit.SECONDS)).length);

        faulty.release();
    }

    @Test
    public void testHungDiskReadFromCacheIsNotBlockedByStuckWrite() throws Exception {
        // The proxy's read path serves downstream consumers; it must not be dragged down by a
        // wedged flush. preferCacheRead answers from the cache without consulting in-flight IO.
        FaultyDelegate faulty = newFaultyDelegate();
        TailCacheFileSystem hangTcf = newTcf(faulty, hangConfig());
        String p = path("hang_read");
        AsyncFile writer = hangTcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();

        hangTcf.write(writer, bufOf(new byte[]{1, 2, 3, 4, 5})).get(5, TimeUnit.SECONDS);
        faulty.hangOn(Op.FILE_WRITE);
        // Push past writeBatchBytes so a flush is in flight and stuck.
        hangTcf.write(writer, bufOf(new byte[200])).get(5, TimeUnit.SECONDS);

        long start = System.nanoTime();
        byte[] head = readBytes(hangTcf.read(writer, 5, 0).get(5, TimeUnit.SECONDS));
        long elapsedMs = (System.nanoTime() - start) / 1_000_000L;

        assertArrayEquals(new byte[]{1, 2, 3, 4, 5}, head);
        assertTrue("cache read must not wait on the stuck flush, took " + elapsedMs + "ms",
                elapsedMs < BOUNDED_MS);

        faulty.release();
    }

    @Test
    public void testHungDiskFsyncFailsBoundedRatherThanHanging() throws Exception {
        FaultyDelegate faulty = newFaultyDelegate();
        TailCacheFileSystem hangTcf = newTcf(faulty, hangConfig());
        String p = path("hang_fsync");
        AsyncFile writer = hangTcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();

        faulty.hangOn(Op.FILE_WRITE);
        hangTcf.write(writer, bufOf(new byte[200])).get(5, TimeUnit.SECONDS);

        // fsync waits on the in-flight flush; awaitFuture turns the timeout into
        // OperationNotExecutedException, which is the caller's signal to retry.
        long start = System.nanoTime();
        try {
            hangTcf.fsync(writer).get(5, TimeUnit.SECONDS);
            fail("expected OperationNotExecutedException");
        } catch (OperationNotExecutedException expected) {
            // thrown synchronously by awaitInFlightIo
        }
        long elapsedMs = (System.nanoTime() - start) / 1_000_000L;
        assertTrue("fsync must give up within its budget, took " + elapsedMs + "ms",
                elapsedMs < BOUNDED_MS);

        faulty.release();
    }

    @Test
    public void testHungDiskCloseUnderAsyncFailsBounded() throws Exception {
        // Records today's behaviour: closeInternal calls awaitInFlightIo *outside* its try block,
        // so a wedged flush makes close throw and the channels are not detached. The way to close
        // a handle while the disk is wedged is to switch to NO_FS first — see
        // testHungDiskSwitchToNoFsRestoresAvailability.
        FaultyDelegate faulty = newFaultyDelegate();
        TailCacheFileSystem hangTcf = newTcf(faulty, hangConfig());
        String p = path("hang_close");
        AsyncFile writer = hangTcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();

        faulty.hangOn(Op.FILE_WRITE);
        hangTcf.write(writer, bufOf(new byte[200])).get(5, TimeUnit.SECONDS);

        long start = System.nanoTime();
        try {
            hangTcf.close(writer).get(5, TimeUnit.SECONDS);
            fail("expected OperationNotExecutedException from close on a wedged disk");
        } catch (OperationNotExecutedException expected) {
            // bounded failure rather than an unbounded wait
        }
        long elapsedMs = (System.nanoTime() - start) / 1_000_000L;
        assertTrue("close must give up within its budget, took " + elapsedMs + "ms",
                elapsedMs < BOUNDED_MS);
        assertFalse("close did not complete, so the handle is still open", writer.closed);

        faulty.release();
        // Once the disk recovers the same handle can still be closed normally.
        hangTcf.close(writer).get(5, TimeUnit.SECONDS);
        assertTrue(writer.closed);
    }

    @Test
    public void testHungDiskOpenReturnsBoundedWithUninitializedCache() throws Exception {
        // open needs sizeSync to seed the tail cache. When that wedges, awaitIoCachePrep times out
        // and initStorageCache swallows it, so open still returns — with an uninitialized cache.
        FaultyDelegate faulty = newFaultyDelegate();
        TailCacheFileSystem hangTcf = newTcf(faulty, hangConfig());
        String p = path("hang_open");
        writeFileSync(p, new byte[]{1, 2, 3});

        faulty.hangOn(Op.FILE_SIZE);
        long start = System.nanoTime();
        AsyncFile writer = hangTcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        long elapsedMs = (System.nanoTime() - start) / 1_000_000L;

        assertTrue("open must return within its budget, took " + elapsedMs + "ms",
                elapsedMs < BOUNDED_MS);
        FileCacheEntry entry = writer.getCacheEntry();
        assertFalse("cache could not be seeded from the wedged disk", entry.isInitialized());

        // With no cache to fall back on, NO_FS cannot accept the write either — it says so
        // explicitly instead of pretending to buffer it.
        hangTcf.setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.NO_FS);
        try {
            hangTcf.write(writer, bufOf(new byte[]{9}));
            fail("expected CannotInitCacheInNoFsException");
        } catch (CannotInitCacheInNoFsException expected) {
            // the cache was never initialized, so there is nowhere to put the data
        }

        faulty.release();
    }

    @Test
    public void testHungDiskSwitchToNoFsRestoresAvailability() throws Exception {
        // End-to-end: the disk wedges, ASYNC operations start failing, the operator flips to NO_FS
        // and the pipeline keeps serving from memory — open, write, read and close all work again.
        FaultyDelegate faulty = newFaultyDelegate();
        TailCacheFileSystem hangTcf = newTcf(faulty, hangConfig());
        String p = path("hang_switch_nofs");
        AsyncFile writer = hangTcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();

        faulty.hangOn(Op.FILE_WRITE);
        hangTcf.write(writer, bufOf(new byte[200])).get(5, TimeUnit.SECONDS);
        // Confirm we really are in the degraded state before switching.
        try {
            hangTcf.fsync(writer).get(5, TimeUnit.SECONDS);
            fail("expected the wedged disk to break fsync under ASYNC");
        } catch (OperationNotExecutedException expected) {
            // expected
        }

        hangTcf.setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.NO_FS);

        long start = System.nanoTime();
        hangTcf.write(writer, bufOf(new byte[]{7, 8, 9})).get(5, TimeUnit.SECONDS);
        assertEquals(203, writer.getCacheEntry().cacheEndOffset);
        assertArrayEquals(new byte[]{7, 8, 9},
                readBytes(hangTcf.read(writer, 3, 200).get(5, TimeUnit.SECONDS)));
        hangTcf.fsync(writer).get(5, TimeUnit.SECONDS);   // no-op, must not throw
        hangTcf.close(writer).get(5, TimeUnit.SECONDS);   // skips the flush, so it completes
        long elapsedMs = (System.nanoTime() - start) / 1_000_000L;

        assertTrue(writer.closed);
        assertTrue("NO_FS operations must not touch the wedged disk at all, took " + elapsedMs + "ms",
                elapsedMs < BOUNDED_MS);

        // A fresh writer can be opened and used while the disk is still wedged.
        AsyncFile reopened = hangTcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        try {
            hangTcf.write(reopened, bufOf(new byte[]{1, 2})).get(5, TimeUnit.SECONDS);
            assertArrayEquals(new byte[]{1, 2},
                    readBytes(hangTcf.read(reopened, 2, 0).get(5, TimeUnit.SECONDS)));
        } finally {
            hangTcf.close(reopened).get(5, TimeUnit.SECONDS);
        }

        faulty.release();
    }

    // =========================================================================
    // E3. Switching between ASYNC and NO_FS at runtime (setBackingFsMode is read per call).
    // Two dirty dimensions are in play: file.needPrepare (no channel / dir may be missing) and
    // entry.fsInconsistent (disk disagrees with cache). restoreBackingFsAndAwait repairs both, but
    // only for writers — needApply is (canWrite() && fsInconsistent).
    // =========================================================================

    @Test
    public void testAsyncToNoFsWriteOnlyIsBackfilledOnReturn() throws Exception {
        // A plain write under NO_FS does not set fsInconsistent: it only withholds the flush, so
        // writtenToFsOffset still marks a truthful boundary. On return to ASYNC the normal
        // flush path resumes from that boundary and no repair is needed.
        TailCacheFileSystem tcfSwitch = newTcf(baseConfig());
        String p = path("switch_write_only");
        AsyncFile writer = tcfSwitch.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        try {
            tcfSwitch.write(writer, bufOf(bytes(1, 2, 3))).get(5, TimeUnit.SECONDS);
            tcfSwitch.fsync(writer).get(5, TimeUnit.SECONDS);
            FileCacheEntry entry = writer.getCacheEntry();
            assertEquals(3, entry.writtenToFsOffset);

            tcfSwitch.setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.NO_FS);
            tcfSwitch.write(writer, bufOf(bytes(4, 5))).get(5, TimeUnit.SECONDS);
            tcfSwitch.write(writer, bufOf(bytes(6, 7))).get(5, TimeUnit.SECONDS);
            assertEquals(7, entry.cacheEndOffset);
            assertEquals("flush boundary stays truthful", 3, entry.writtenToFsOffset);
            assertFalse("a withheld flush is not an inconsistency", entry.fsInconsistent);
            assertArrayEquals(bytes(1, 2, 3), readFileSync(p));

            tcfSwitch.setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.ASYNC);
            // flushPendingWriteAndAwait collects with maxBytes=Long.MAX_VALUE, so one fsync drains
            // everything buffered during the outage regardless of writeBatchBytes.
            tcfSwitch.fsync(writer).get(5, TimeUnit.SECONDS);
            awaitAll();
            assertEquals(7, entry.writtenToFsOffset);
            assertArrayEquals(bytes(1, 2, 3, 4, 5, 6, 7), readFileSync(p));
        } finally {
            tcfSwitch.setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.ASYNC);
            tcfSwitch.close(writer).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testAsyncToNoFsRollIsRepairedOnReturn() throws Exception {
        // roll moves segment metadata that the disk knows nothing about, so it does set
        // fsInconsistent. Returning to ASYNC must materialise the rolled segment.
        TailCacheFileSystem tcfSwitch = newTcf(baseConfig());
        String dir = path("switch_roll");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile writer = tcfSwitch.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        try {
            tcfSwitch.write(writer, bufOf(new byte[10])).get(5, TimeUnit.SECONDS);
            tcfSwitch.fsync(writer).get(5, TimeUnit.SECONDS);

            tcfSwitch.setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.NO_FS);
            tcfSwitch.roll(writer).get(5, TimeUnit.SECONDS);
            tcfSwitch.write(writer, bufOf(new byte[20])).get(5, TimeUnit.SECONDS);
            SegmentFileCacheEntry entry = writer.getCacheEntry();
            assertTrue(entry.fsInconsistent);
            assertEquals(Arrays.asList(0L, 10L), tcfSwitch.list(writer));
            assertFalse(Files.exists(Paths.get(dir, SEG_PREFIX + "10")));

            tcfSwitch.setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.ASYNC);
            tcfSwitch.fsync(writer).get(5, TimeUnit.SECONDS);
            awaitAll();

            assertFalse("restore cleared the inconsistency", entry.fsInconsistent);
            assertEquals("whole range trusted again", 0, entry.localReadableFromOffset);
            assertEquals(30, entry.writtenToFsOffset);
            assertEquals(10, Files.size(Paths.get(dir, SEG_PREFIX + "0")));
            assertEquals(20, Files.size(Paths.get(dir, SEG_PREFIX + "10")));
        } finally {
            tcfSwitch.setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.ASYNC);
            tcfSwitch.close(writer).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testAsyncToNoFsTruncateRealignsLastSegmentOnReturn() throws Exception {
        // truncate under NO_FS shrinks the cache while the file on disk keeps its old length.
        // alignLastSegmentForRestore truncates the tail segment down to the calibrated offset.
        TailCacheFileSystem tcfSwitch = newTcf(baseConfig());
        String dir = path("switch_truncate");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile writer = tcfSwitch.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        try {
            tcfSwitch.write(writer, bufOf(new byte[30])).get(5, TimeUnit.SECONDS);
            tcfSwitch.fsync(writer).get(5, TimeUnit.SECONDS);
            assertEquals(30, Files.size(Paths.get(dir, SEG_PREFIX + "0")));

            tcfSwitch.setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.NO_FS);
            tcfSwitch.truncate(writer, 20).get(5, TimeUnit.SECONDS);
            SegmentFileCacheEntry entry = writer.getCacheEntry();
            assertEquals(20, entry.cacheEndOffset);
            assertTrue(entry.fsInconsistent);
            assertEquals("disk still holds the pre-truncate length",
                    30, Files.size(Paths.get(dir, SEG_PREFIX + "0")));

            tcfSwitch.setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.ASYNC);
            tcfSwitch.fsync(writer).get(5, TimeUnit.SECONDS);
            awaitAll();

            assertFalse(entry.fsInconsistent);
            assertEquals(20, entry.writtenToFsOffset);
            assertEquals("tail segment realigned to the truncated length",
                    20, Files.size(Paths.get(dir, SEG_PREFIX + "0")));
        } finally {
            tcfSwitch.setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.ASYNC);
            tcfSwitch.close(writer).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testAsyncToNoFsDeleteSegmentsLeavesOrphansCleanedOnReturn() throws Exception {
        // deleteSegments under NO_FS drops metadata only, so the files linger. Step 1 of the
        // restore (deleteOrphanSegmentFilesSync) is what eventually unlinks them.
        TailCacheFileSystem tcfSwitch = newTcf(baseConfig());
        String dir = path("switch_delete_segments");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile writer = tcfSwitch.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        try {
            tcfSwitch.write(writer, bufOf(new byte[10])).get(5, TimeUnit.SECONDS);
            tcfSwitch.roll(writer).get(5, TimeUnit.SECONDS);
            tcfSwitch.write(writer, bufOf(new byte[10])).get(5, TimeUnit.SECONDS);
            tcfSwitch.roll(writer).get(5, TimeUnit.SECONDS);
            tcfSwitch.write(writer, bufOf(new byte[10])).get(5, TimeUnit.SECONDS);
            tcfSwitch.fsync(writer).get(5, TimeUnit.SECONDS);
            awaitAll();
            assertTrue(Files.exists(Paths.get(dir, SEG_PREFIX + "0")));

            tcfSwitch.setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.NO_FS);
            tcfSwitch.deleteSegments(writer, Collections.singletonList(0L)).get(5, TimeUnit.SECONDS);
            assertEquals(Arrays.asList(10L, 20L), tcfSwitch.list(writer));
            assertTrue("orphan still on disk under NO_FS",
                    Files.exists(Paths.get(dir, SEG_PREFIX + "0")));

            tcfSwitch.setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.ASYNC);
            tcfSwitch.fsync(writer).get(5, TimeUnit.SECONDS);
            awaitAll();

            assertFalse("restore unlinked the orphan", Files.exists(Paths.get(dir, SEG_PREFIX + "0")));
            assertTrue(Files.exists(Paths.get(dir, SEG_PREFIX + "10")));
            assertTrue(Files.exists(Paths.get(dir, SEG_PREFIX + "20")));
            assertFalse(writer.getCacheEntry().fsInconsistent);
        } finally {
            tcfSwitch.setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.ASYNC);
            tcfSwitch.close(writer).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testNoFsOpenedWriterPreparesDirectoryAndChannelOnReturn() throws Exception {
        // A handle opened while NO_FS was active has needPrepare set and no channel — its directory
        // may not even exist. prepareFileSync (mkdir + openCurrentChannel) runs on the first
        // ASYNC operation, before any repair.
        TailCacheFileSystem tcfSwitch = newTcf(
                baseConfig().setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.NO_FS));
        String dir = path("switch_prepare");
        String p = Paths.get(dir, "file").toString();
        assertFalse("directory does not exist yet", Files.exists(Paths.get(dir)));

        AsyncFile writer = tcfSwitch.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        try {
            assertTrue(writer.needPrepare);
            FileCacheEntry entry = writer.getCacheEntry();
            assertTrue(entry.fsInconsistent);

            tcfSwitch.write(writer, bufOf(bytes(1, 2, 3, 4, 5))).get(5, TimeUnit.SECONDS);
            assertFalse("still nothing on disk", Files.exists(Paths.get(dir)));

            tcfSwitch.setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.ASYNC);
            tcfSwitch.fsync(writer).get(5, TimeUnit.SECONDS);
            awaitAll();

            assertFalse("channel prepared", writer.needPrepare);
            assertFalse(entry.fsInconsistent);
            assertTrue(Files.isDirectory(Paths.get(dir)));
            assertArrayEquals(bytes(1, 2, 3, 4, 5), readFileSync(p));
        } finally {
            tcfSwitch.setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.ASYNC);
            tcfSwitch.close(writer).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testReaderDoesNotClearInconsistencyOnlyWriterDoes() throws Exception {
        // needApply requires canWrite(), so a reader sharing the entry cannot repair it. Until the
        // writer runs a restore the reader stays cache-only, even back under ASYNC.
        TailCacheFileSystem tcfSwitch = newTcf(baseConfig());
        String dir = path("switch_reader_repair");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile writer = tcfSwitch.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        AsyncSegmentFile reader = tcfSwitch.open(dir, SEG_PREFIX, INDEX_PREFIXES, false, null).get();
        try {
            tcfSwitch.write(writer, bufOf(new byte[30])).get(5, TimeUnit.SECONDS);
            tcfSwitch.fsync(writer).get(5, TimeUnit.SECONDS);
            awaitAll();

            tcfSwitch.setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.NO_FS);
            tcfSwitch.truncate(writer, 20).get(5, TimeUnit.SECONDS);
            SegmentFileCacheEntry shared = writer.getCacheEntry();
            assertSame("writer and reader share the entry", shared, reader.getCacheEntry());
            assertTrue(shared.fsInconsistent);

            tcfSwitch.setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.ASYNC);
            // Reader-only traffic leaves the flag alone.
            tcfSwitch.size(reader).get(5, TimeUnit.SECONDS);
            assertTrue("a reader must not clear fsInconsistent", shared.fsInconsistent);

            // Now let the writer repair it.
            tcfSwitch.fsync(writer).get(5, TimeUnit.SECONDS);
            awaitAll();
            assertFalse(shared.fsInconsistent);
        } finally {
            tcfSwitch.setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.ASYNC);
            tcfSwitch.close(reader).get(5, TimeUnit.SECONDS);
            tcfSwitch.close(writer).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testRestoreReenablesDiskDegradedReads() throws Exception {
        // With readPreferCache=false, an already-flushed offset normally goes to disk. While
        // fsInconsistent is set the same read is pinned to the shared cache, and after the writer
        // restores the backing FS the reader may use disk again. Use a READ handle because a writer's
        // segment channel is intentionally write-only.
        TailCacheFileSystem tcfSwitch = newTcf(baseConfig());
        tcfSwitch.setReadPreferCache(false);
        String dir = path("switch_disk_reads");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile writer = tcfSwitch.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        AsyncSegmentFile reader = tcfSwitch.open(dir, SEG_PREFIX, INDEX_PREFIXES, false, null).get();
        try {
            tcfSwitch.write(writer, bufOf(new byte[30])).get(5, TimeUnit.SECONDS);
            tcfSwitch.fsync(writer).get(5, TimeUnit.SECONDS);
            awaitAll();
            SegmentFileCacheEntry entry = writer.getCacheEntry();
            assertSame("reader and writer must observe the same consistency state",
                    entry, reader.getCacheEntry());

            // Baseline: consistent entry, offset below writtenToFsOffset -> disk.
            delegate.reset();
            ByteBuf baseline = tcfSwitch.read(reader, 10, 0).get(5, TimeUnit.SECONDS);
            assertEquals(10, baseline.readableBytes());
            baseline.release();
            assertTrue("a consistent entry degrades to disk", delegate.segReadCount > 0);

            tcfSwitch.setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.NO_FS);
            tcfSwitch.truncate(writer, 20).get(5, TimeUnit.SECONDS);
            assertTrue(entry.fsInconsistent);
            tcfSwitch.setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.ASYNC);

            // Same read, but the disk is no longer trustworthy: served from cache.
            delegate.reset();
            ByteBuf pinned = tcfSwitch.read(reader, 10, 0).get(5, TimeUnit.SECONDS);
            assertEquals(10, pinned.readableBytes());
            pinned.release();
            assertEquals("an inconsistent entry must not read the disk", 0, delegate.segReadCount);

            // Repair through the writer, then the reader is allowed onto disk again.
            tcfSwitch.fsync(writer).get(5, TimeUnit.SECONDS);
            awaitAll();
            assertFalse(entry.fsInconsistent);
            assertEquals(0, entry.localReadableFromOffset);

            delegate.reset();
            ByteBuf afterRestore = tcfSwitch.read(reader, 10, 0).get(5, TimeUnit.SECONDS);
            assertEquals(10, afterRestore.readableBytes());
            afterRestore.release();
            assertTrue("restore re-enabled disk reads", delegate.segReadCount > 0);
        } finally {
            tcfSwitch.setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.ASYNC);
            tcfSwitch.close(reader).get(5, TimeUnit.SECONDS);
            tcfSwitch.close(writer).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testRestorePartialSuccessMarksLocalReadableBoundary() throws Exception {
        // When a historical segment can no longer be rebuilt (its bytes were evicted from the
        // cache), restore stops there and reports the boundary via localReadableFromOffset instead
        // of failing. fsInconsistent is still cleared — the boundary is the record of the damage.
        //
        // Layout built below (chunk=64, minRetain=1, maxCacheSizeBytes=200, retention=0):
        //   ASYNC  write 64B + fsync      -> state=[0],        written=64, chunks={0}
        //   NO_FS  roll                   -> state=[0,64]
        //          write 64B              -> maxEvictable=1-1=0, chunks={0,1}, cacheEnd=128
        //          roll                   -> state=[0,64,128]
        //          write 64B              -> maxEvictable=2-1=1, ratio=128/200>=high -> evict chunk0
        //                                    cacheStart=64,  chunks={1,2}, cacheEnd=192
        //          write 64B              -> evict chunk1 (allowDirtyEvict=true under NO_FS)
        //                                    cacheStart=128, chunks={2,3}, cacheEnd=256
        // Restore then finds written(64) < lastStart(128) and walks back to segment 64, whose
        // logicalFrom is 64 < cacheStartOffset(128) -> dataSupplier returns null -> stop.
        TailCacheFileSystem tightTcf = newTcf(
                evictConfig(1, 200, 0).setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.ASYNC));
        String dir = path("switch_partial_restore");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile writer = tightTcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        try {
            SegmentFileCacheEntry entry = writer.getCacheEntry();
            tightTcf.write(writer, bufOf(new byte[(int) CHUNK_SIZE])).get(5, TimeUnit.SECONDS);
            tightTcf.fsync(writer).get(5, TimeUnit.SECONDS);
            assertEquals(CHUNK_SIZE, entry.writtenToFsOffset);

            tightTcf.setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.NO_FS);
            tightTcf.roll(writer).get(5, TimeUnit.SECONDS);
            tightTcf.write(writer, bufOf(new byte[(int) CHUNK_SIZE])).get(5, TimeUnit.SECONDS);
            tightTcf.roll(writer).get(5, TimeUnit.SECONDS);
            tightTcf.write(writer, bufOf(new byte[(int) CHUNK_SIZE])).get(5, TimeUnit.SECONDS);
            tightTcf.write(writer, bufOf(new byte[(int) CHUNK_SIZE])).get(5, TimeUnit.SECONDS);

            assertEquals(Arrays.asList(0L, CHUNK_SIZE, 2 * CHUNK_SIZE), tightTcf.list(writer));
            assertEquals("eviction moved the window past segment 64",
                    2 * CHUNK_SIZE, entry.cacheStartOffset);
            assertEquals(CHUNK_SIZE, entry.writtenToFsOffset);

            tightTcf.setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.ASYNC);
            tightTcf.fsync(writer).get(5, TimeUnit.SECONDS);
            awaitAll();

            assertFalse("partial success still clears the flag", entry.fsInconsistent);
            assertEquals("boundary records what could not be rebuilt",
                    2 * CHUNK_SIZE, entry.localReadableFromOffset);
            // rewriteRangeSync truncates before asking the cache, so the unrepairable segment is
            // left empty rather than holding bytes nobody vouches for.
            assertEquals(0, Files.size(Paths.get(dir, SEG_PREFIX + String.valueOf(CHUNK_SIZE))));

            // Below the boundary and outside the cache window there is nothing left to serve.
            try {
                tightTcf.read(writer, 8, CHUNK_SIZE + 8);
                fail("expected CannotReadPositionInNoFsException below localReadableFromOffset");
            } catch (CannotReadPositionInNoFsException expected) {
                // the range is gone from both cache and disk
            }
            // At and above the boundary the cache still serves.
            assertEquals(8, readBytes(
                    tightTcf.read(writer, 8, 2 * CHUNK_SIZE).get(5, TimeUnit.SECONDS)).length);
        } finally {
            tightTcf.setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.ASYNC);
            tightTcf.close(writer).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testRestoreTimeoutFailsRollAndRetryIsIdempotent() throws Exception {
        // A restore that times out leaves the entry dirty and makes roll report
        // OperationNotExecutedException. Note the metadata phase already ran, so the caller's
        // contract is "retry", and the retry must not add a second segment.
        FaultyDelegate faulty = newFaultyDelegate();
        TailCacheFileSystem hangTcf = newTcf(faulty, hangConfig());
        String dir = path("switch_restore_timeout");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile writer = hangTcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        try {
            hangTcf.write(writer, bufOf(new byte[10])).get(5, TimeUnit.SECONDS);
            hangTcf.fsync(writer).get(5, TimeUnit.SECONDS);

            hangTcf.setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.NO_FS);
            hangTcf.roll(writer).get(5, TimeUnit.SECONDS);
            hangTcf.write(writer, bufOf(new byte[20])).get(5, TimeUnit.SECONDS);
            SegmentFileCacheEntry entry = writer.getCacheEntry();
            assertTrue(entry.fsInconsistent);

            // Wedge a step the restore must pass through, then go back to ASYNC.
            faulty.hangOn(Op.ORPHAN_SCAN);
            hangTcf.setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.ASYNC);
            try {
                hangTcf.roll(writer);
                fail("expected OperationNotExecutedException when restore times out");
            } catch (OperationNotExecutedException expected) {
                // caller is expected to retry
            }
            assertTrue("nothing was applied", entry.fsInconsistent);
            List<Long> afterFailedRoll = hangTcf.list(writer);
            assertEquals("the metadata phase runs before the restore",
                    Arrays.asList(0L, 10L, 30L), afterFailedRoll);

            faulty.release();
            // Retry: the tail segment is empty now, so rollMetadata is a no-op and only the
            // restore is redone.
            hangTcf.roll(writer).get(5, TimeUnit.SECONDS);
            awaitAll();
            assertEquals("retry must not add another segment", afterFailedRoll, hangTcf.list(writer));
            assertFalse(entry.fsInconsistent);
        } finally {
            hangTcf.setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.ASYNC);
            faulty.release();
            hangTcf.close(writer).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testRestoreFailureDegradesTailCacheWriteButFailsAtomicReplace() throws Exception {
        // A tail-cache writer can absorb a failed restore: the bytes stay in the cache and
        // writtenToFsOffset still describes a real boundary, so write returns normally. An
        // atomicReplace writer has no partial-flush state to fall back on, so it must fail loudly.
        FaultyDelegate faulty = newFaultyDelegate();
        TailCacheFileSystem hangTcf = newTcf(faulty, hangConfig()
                .setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.NO_FS));
        String dir = path("switch_restore_fail");
        String tailPath = Paths.get(dir, "tail").toString();
        String atomicPath = Paths.get(dir, "atomic").toString();

        AsyncFile tailWriter = hangTcf.open(tailPath, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        AsyncFile atomicWriter = hangTcf.open(atomicPath, AbstractStorageFile.OpenMode.WRITE, true, false, null).get();
        try {
            hangTcf.write(tailWriter, bufOf(bytes(1, 2, 3))).get(5, TimeUnit.SECONDS);
            FileCacheEntry tailEntry = tailWriter.getCacheEntry();
            assertTrue(tailEntry.fsInconsistent);

            // Both handles still need prepare, so make mkdir wedge and return to ASYNC.
            faulty.hangOn(Op.MKDIR);
            hangTcf.setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.ASYNC);

            long accepted = hangTcf.write(tailWriter, bufOf(bytes(4, 5))).get(5, TimeUnit.SECONDS);
            assertEquals("tail-cache write is accepted despite the failed restore", 2, accepted);
            assertEquals(5, tailEntry.cacheEndOffset);
            assertEquals("nothing reached the disk", 0, tailEntry.writtenToFsOffset);
            assertTrue(tailEntry.fsInconsistent);
            assertArrayEquals(bytes(1, 2, 3, 4, 5),
                    readBytes(hangTcf.read(tailWriter, 5, 0).get(5, TimeUnit.SECONDS)));

            try {
                hangTcf.write(atomicWriter, bufOf(bytes(9, 9)));
                fail("expected OperationNotExecutedException for atomicReplace");
            } catch (OperationNotExecutedException expected) {
                // no partial-flush semantics to degrade to
            }

            // Once the disk comes back the buffered tail data is flushed for real.
            faulty.release();
            hangTcf.fsync(tailWriter).get(5, TimeUnit.SECONDS);
            awaitAll();
            assertFalse(tailEntry.fsInconsistent);
            assertArrayEquals(bytes(1, 2, 3, 4, 5), readFileSync(tailPath));
        } finally {
            faulty.release();
            hangTcf.setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.ASYNC);
            hangTcf.close(tailWriter).get(5, TimeUnit.SECONDS);
            hangTcf.close(atomicWriter).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testNoCacheBackingMode() throws Exception {
        TailCacheFileSystemConfig config = new TailCacheFileSystemConfig();
        config.setPerFileCacheLimits(10 * 1024, 1, CHUNK_SIZE);
        config.setMaxCacheSizeBytes(100 * 1024);
        config.setWriteBatchBytes(128);
        config.setIoWaitTimeoutMs(5000);
        config.setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.NO_CACHE);
        TailCacheFileSystem noCacheTcf = newTcf(config);

        String p = path("file21");
        AsyncFile file = noCacheTcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        try {
            noCacheTcf.write(file, bufOf(new byte[]{7, 8, 9})).get(5, TimeUnit.SECONDS);
            // No cache memory allocated (chunks not used)
            assertEquals(0, noCacheTcf.getGlobalCommittedBytes());
            // Wait for IO to complete
            noCacheTcf.fsync(file).get(5, TimeUnit.SECONDS);
            assertArrayEquals(new byte[]{7, 8, 9}, readFileSync(p));
        } finally {
            noCacheTcf.close(file).get(5, TimeUnit.SECONDS);
        }
    }

    // =========================================================================
    // F. Edge cases & error handling
    // =========================================================================

    @Test(expected = Exception.class)
    public void testWriteOnClosedFileThrows() throws Exception {
        String p = path("file22");
        AsyncFile file = tcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        tcf.close(file).get(5, TimeUnit.SECONDS);
        tcf.write(file, bufOf(new byte[]{1})).get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testDoubleWriterThrows() throws Exception {
        String p = path("file23");
        AsyncFile writer1 = tcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        try {
            tcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
            fail("should have thrown");
        } catch (Exception e) {
            // open() now runs openFileSync on the calling thread and returns an already
            // completed future, so the writer-exclusion error surfaces synchronously.
            assertTrue(e instanceof IllegalStateException
                    || e.getCause() instanceof IllegalStateException);
        } finally {
            tcf.close(writer1).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testMultipleReadersAllowed() throws Exception {
        String p = path("file24");
        writeFileSync(p, new byte[]{1, 2, 3});
        AsyncFile reader1 = tcf.open(p, AbstractStorageFile.OpenMode.READ, false, false, null).get();
        AsyncFile reader2 = tcf.open(p, AbstractStorageFile.OpenMode.READ, false, false, null).get();
        try {
            byte[] d1 = readTcfSync(reader1, 3);
            byte[] d2 = readTcfSync(reader2, 3);
            assertArrayEquals(new byte[]{1, 2, 3}, d1);
            assertArrayEquals(new byte[]{1, 2, 3}, d2);
        } finally {
            tcf.close(reader1).get(5, TimeUnit.SECONDS);
            tcf.close(reader2).get(5, TimeUnit.SECONDS);
        }
    }

    @Test(expected = Exception.class)
    public void testWriteToReadModeThrows() throws Exception {
        String p = path("file25");
        writeFileSync(p, new byte[]{1});
        AsyncFile file = tcf.open(p, AbstractStorageFile.OpenMode.READ, false, false, null).get();
        try {
            tcf.write(file, bufOf(new byte[]{2})).get(5, TimeUnit.SECONDS);
        } finally {
            tcf.close(file).get(5, TimeUnit.SECONDS);
        }
    }

    // =========================================================================
    // G. preferCacheRead & read path branches
    // =========================================================================

    @Test
    public void testPreferCacheReadNoCache() throws Exception {
        // NO_CACHE → not in cache; local readable → (false, true)
        String p = path("file_pdr_nocache");
        writeFileSync(p, new byte[10]); // file must exist for READ mode open
        AsyncFile file = tcf.open(p, AbstractStorageFile.OpenMode.READ, false, false, null,
                AbstractStorageFile.CacheMode.NO_CACHE).get();
        FileCacheEntry entry = file.getCacheEntry();
        Pair<Boolean, Boolean> d = tcf.preferCacheRead(file, entry, 0, true, tcf.getBackingFsMode());
        assertFalse(d.getKey());
        assertTrue(d.getValue());
        tcf.close(file).get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testPreferCacheReadCacheHitWriter() throws Exception {
        // Writer with data in cache, preferCache=true → (true, true); read hits cache
        String p = path("file_pdr_hit");
        AsyncFile writer = tcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        writeTcfSync(writer, new byte[]{1, 2, 3, 4, 5});
        tcf.fsync(writer).get(5, TimeUnit.SECONDS);
        // Now: cacheEndOffset=5, writtenToFsOffset=5, cacheStartOffset=0
        FileCacheEntry entry = writer.getCacheEntry();
        Pair<Boolean, Boolean> d = tcf.preferCacheRead(writer, entry, 2, true, tcf.getBackingFsMode());
        assertTrue(d.getKey());
        assertTrue(d.getValue());
        // Read from cache should return correct data
        delegate.reset();
        delegate.fileReadCount = 0;
        ByteBuf buf = tcf.read(writer, 3, 2).get(5, TimeUnit.SECONDS);
        try {
            assertArrayEquals(new byte[]{3, 4, 5}, readBytes(buf));
            assertEquals("cache read should not call delegate", 0, delegate.fileReadCount);
        } finally {
        }
        tcf.close(writer).get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testPreferCacheReadLocalReadableFromOffset() throws Exception {
        String p = path("file_pdr_local_from");
        AsyncFile writer = tcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        writeTcfSync(writer, new byte[]{1, 2, 3, 4, 5});
        tcf.fsync(writer).get(5, TimeUnit.SECONDS);
        FileCacheEntry entry = writer.getCacheEntry();
        entry.fsInconsistent = false;
        entry.localReadableFromOffset = 3;

        // In cache, offset before localReadableFromOffset → prefer cache, cannot degrade
        Pair<Boolean, Boolean> inCacheBlocked = tcf.preferCacheRead(writer, entry, 2, true, tcf.getBackingFsMode());
        assertTrue(inCacheBlocked.getKey());
        assertFalse(inCacheBlocked.getValue());

        // In cache, offset at/after localReadableFromOffset → normal (preferCache=true → cache, can degrade)
        Pair<Boolean, Boolean> inCacheOk = tcf.preferCacheRead(writer, entry, 3, true, tcf.getBackingFsMode());
        assertTrue(inCacheOk.getKey());
        assertTrue(inCacheOk.getValue());

        // Not in cache (offset before cacheStart): move cache window up, then offset 0 is out of cache
        entry.cacheStartOffset = 4;
        try {
            tcf.preferCacheRead(writer, entry, 0, true, tcf.getBackingFsMode());
            fail("expected CannotReadPositionInNoFsException");
        } catch (CannotReadPositionInNoFsException expected) {
            // offset 0 < localReadableFromOffset 3 and not in cache
        }

        // Not in cache (3 < cacheStart) but locally readable (3 >= localReadableFromOffset)
        Pair<Boolean, Boolean> diskOk = tcf.preferCacheRead(writer, entry, 3, false, tcf.getBackingFsMode());
        assertFalse(diskOk.getKey());
        assertTrue(diskOk.getValue());

        tcf.close(writer).get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testReadPositionUpdatesOnCacheHit() throws Exception {
        // read(file, length) with fromPosition=true should update file.position on cache hit
        String p = path("file_read_pos");
        AsyncFile writer = tcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        writeTcfSync(writer, new byte[]{10, 20, 30, 40, 50});
        tcf.fsync(writer).get(5, TimeUnit.SECONDS);
        // position starts at 0
        assertEquals(0, writer.position);
        // Read 3 bytes from position — should hit cache (writer has data)
        ByteBuf buf = tcf.read(writer, 3).get(5, TimeUnit.SECONDS);
        try {
            assertArrayEquals(new byte[]{10, 20, 30}, readBytes(buf));
        } finally {
            // position should advance by 3
            assertEquals(3, writer.position);
        }
        // Read 2 more bytes
        ByteBuf buf2 = tcf.read(writer, 2).get(5, TimeUnit.SECONDS);
        try {
            assertArrayEquals(new byte[]{40, 50}, readBytes(buf2));
        } finally {
            assertEquals(5, writer.position);
        }
        tcf.close(writer).get(5, TimeUnit.SECONDS);
    }

    // =========================================================================
    // H. Truncate branches
    // =========================================================================

    @Test
    public void testTruncateAtomicReplaceCache() throws Exception {
        // atomicReplace truncate: allocates new buffer, copies prefix, setAtomicChunk
        String p = path("file_trunc_ar");
        writeFileSync(p, new byte[]{1, 2, 3, 4, 5, 6, 7, 8});
        AsyncFile writer = tcf.open(p, AbstractStorageFile.OpenMode.WRITE, true, false, null).get();
        // FULL_CACHE mode loaded entire file into cache chunk 0
        FileCacheEntry entry = writer.getCacheEntry();
        assertTrue(entry.isInitialized());
        assertEquals(8, entry.cacheEndOffset);

        // Write new data (atomic replace)
        writeTcfSync(writer, new byte[]{10, 20, 30, 40});
        assertEquals(4, entry.cacheEndOffset);

        // Truncate to 2 bytes — should trim atomic chunk
        tcf.truncate(writer, 2).get(5, TimeUnit.SECONDS);
        assertEquals(2, entry.cacheEndOffset);
        // The single atomic chunk holds exactly the truncated prefix
        CacheChunk chunk0 = entry.chunks.get(0L);
        assertNotNull(chunk0);
        byte[] cached = new byte[2];
        chunk0.buffer.getBytes(0, cached);
        assertArrayEquals(new byte[]{10, 20}, cached);

        tcf.close(writer).get(5, TimeUnit.SECONDS);
        // Verify on disk
        assertArrayEquals(new byte[]{10, 20}, readFileSync(p));
    }

    @Test
    public void testTruncateNoCacheChangeWhenSizeGteCacheEnd() throws Exception {
        // truncate with size >= cacheEndOffset → cache not modified
        String p = path("file_trunc_nochg");
        AsyncFile writer = tcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        writeTcfSync(writer, new byte[50]);
        tcf.fsync(writer).get(5, TimeUnit.SECONDS);
        FileCacheEntry entry = writer.getCacheEntry();
        long cacheEndBefore = entry.cacheEndOffset;

        // Truncate to size > cacheEndOffset → cache unchanged
        tcf.truncate(writer, 100).get(5, TimeUnit.SECONDS);
        assertEquals(cacheEndBefore, entry.cacheEndOffset);
        tcf.close(writer).get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testTruncateCacheToStart() throws Exception {
        // truncate to 0 → releaseAllChunks, cacheStartOffset=0, cacheEndOffset=0
        String p = path("file_trunc_zero");
        AsyncFile writer = tcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        writeTcfSync(writer, new byte[(int) (CHUNK_SIZE * 2)]);
        tcf.fsync(writer).get(5, TimeUnit.SECONDS);
        FileCacheEntry entry = writer.getCacheEntry();
        assertTrue(entry.chunks.size() > 0);

        tcf.truncate(writer, 0).get(5, TimeUnit.SECONDS);
        assertEquals(0, entry.cacheStartOffset);
        assertEquals(0, entry.cacheEndOffset);
        assertEquals(0, entry.chunks.size());
        tcf.close(writer).get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testTruncateSegmentResetsWhenOffsetBeforeStart() throws Exception {
        // SegmentFileCacheEntry.truncateTo: offset < prevStartOffset → resetSegmentCache
        String dir = path("seg_trunc_reset");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = tcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        tcf.write(seg, bufOf(new byte[10])).get(5, TimeUnit.SECONDS);
        tcf.roll(seg).get(5, TimeUnit.SECONDS);
        tcf.write(seg, bufOf(new byte[20])).get(5, TimeUnit.SECONDS);
        tcf.fsync(seg).get(5, TimeUnit.SECONDS);

        // Close and reopen to get initialized cache at endOffset=30
        tcf.close(seg).get(5, TimeUnit.SECONDS);
        AsyncSegmentFile seg2 = tcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        FileCacheEntry entry = seg2.getCacheEntry();
        assertTrue(entry.isInitialized());
        assertEquals(30, entry.cacheEndOffset);

        // Truncate to -100 (before firstOffset=0) → resetSegmentCache(-100, -100)
        tcf.truncate(seg2, -100).get(5, TimeUnit.SECONDS);
        assertEquals(-100, entry.cacheStartOffset);
        assertEquals(-100, entry.cacheEndOffset);
        tcf.close(seg2).get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testTruncateSegmentNoOpAtCacheEnd() throws Exception {
        // SegmentFileCacheEntry.truncateTo: offset == cacheEndOffset → nothing happens
        String dir = path("seg_trunc_noop");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = tcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        tcf.write(seg, bufOf(new byte[50])).get(5, TimeUnit.SECONDS);
        tcf.fsync(seg).get(5, TimeUnit.SECONDS);

        tcf.close(seg).get(5, TimeUnit.SECONDS);
        AsyncSegmentFile seg2 = tcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        FileCacheEntry entry = seg2.getCacheEntry();
        long cacheEndBefore = entry.cacheEndOffset;
        long writtenBefore = entry.writtenToFsOffset;

        // Truncate at exactly cacheEndOffset → no-op in cache
        tcf.truncate(seg2, cacheEndBefore).get(5, TimeUnit.SECONDS);
        assertEquals(cacheEndBefore, entry.cacheEndOffset);
        assertEquals(writtenBefore, entry.writtenToFsOffset);
        tcf.close(seg2).get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testTruncateSegmentAtOrBeforeCacheStartUpdatesLocalReadable() {
        SegmentFileCacheEntry entry = new SegmentFileCacheEntry(new CacheMemoryTracker());
        entry.cacheStartOffset = 80;
        entry.cacheEndOffset = 200;
        entry.writtenToFsOffset = 120;
        entry.localReadableFromOffset = 100;

        synchronized (entry) {
            entry.truncateTo(70, CHUNK_SIZE, 0);
        }

        assertEquals(0, entry.localReadableFromOffset);
        assertTrue(entry.fsInconsistent);
    }

    @Test
    public void testTruncateSegmentOutsideCachedEndsClearsLocalReadable() {
        SegmentFileCacheEntry entry = new SegmentFileCacheEntry(new CacheMemoryTracker());
        entry.cacheStartOffset = 80;
        entry.cacheEndOffset = 200;
        entry.writtenToFsOffset = 120;
        entry.localReadableFromOffset = 100;

        synchronized (entry) {
            entry.truncateTo(250, CHUNK_SIZE, 0);
        }

        assertEquals(0, entry.localReadableFromOffset);
        assertFalse(entry.fsInconsistent);
    }

    // =========================================================================
    // I. Size reporting branches
    // =========================================================================

    @Test
    public void testSizeOfSegmentNotLast() throws Exception {
        // Non-last segment → size = nextOffset - startOffset (no cache lookup)
        String dir = path("seg_size_notlast");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile writer = tcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        tcf.write(writer, bufOf(new byte[10])).get(5, TimeUnit.SECONDS);
        tcf.roll(writer).get(5, TimeUnit.SECONDS);
        tcf.write(writer, bufOf(new byte[20])).get(5, TimeUnit.SECONDS);
        tcf.roll(writer).get(5, TimeUnit.SECONDS);
        tcf.write(writer, bufOf(new byte[30])).get(5, TimeUnit.SECONDS);
        tcf.close(writer).get(5, TimeUnit.SECONDS);

        AsyncSegmentFile reader = tcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, false, null).get();
        try {
            // Segment 0 (non-last): size should be nextOffset(10) - startOffset(0) = 10
            long size0 = tcf.sizeOfSegment(reader, 0).get(5, TimeUnit.SECONDS);
            assertEquals(10, size0);
            // Segment 1 (non-last): size should be nextOffset(30) - startOffset(10) = 20
            long size1 = tcf.sizeOfSegment(reader, 10).get(5, TimeUnit.SECONDS);
            assertEquals(20, size1);
        } finally {
            tcf.close(reader).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSizeOfSegmentOffsetNotFound() throws Exception {
        // Offset not in offsets list → returns 0
        String dir = path("seg_size_notfound");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile writer = tcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        tcf.write(writer, bufOf(new byte[10])).get(5, TimeUnit.SECONDS);
        tcf.close(writer).get(5, TimeUnit.SECONDS);

        AsyncSegmentFile reader = tcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, false, null).get();
        try {
            long size = tcf.sizeOfSegment(reader, 999).get(5, TimeUnit.SECONDS);
            assertEquals(0, size);
        } finally {
            tcf.close(reader).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSizeOfSegmentLastFromCache() throws Exception {
        // Last segment + initialized → size from cache (max(writtenToFsOffset, cacheEndOffset) - startOffset)
        String dir = path("seg_size_cache");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile writer = tcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        tcf.write(writer, bufOf(new byte[10])).get(5, TimeUnit.SECONDS);
        tcf.roll(writer).get(5, TimeUnit.SECONDS);
        tcf.write(writer, bufOf(new byte[25])).get(5, TimeUnit.SECONDS);
        // Don't fsync — last segment data is in cache but writtenToFsOffset might differ
        FileCacheEntry entry = writer.getCacheEntry();
        long lastSegSize = tcf.size(writer).get(5, TimeUnit.SECONDS) - 10;
        // sizeOfSegment for last segment (offset 10) should use cache
        long segSize = tcf.sizeOfSegment(writer, 10).get(5, TimeUnit.SECONDS);
        assertEquals(lastSegSize, segSize);
        tcf.close(writer).get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testSizeNoCacheFallback() throws Exception {
        // NO_CACHE mode → size delegates to FS, no cache memory used
        String p = path("file_size_nocache");
        writeFileSync(p, new byte[42]);
        AsyncFile file = tcf.open(p, AbstractStorageFile.OpenMode.READ, false, false, null,
                AbstractStorageFile.CacheMode.NO_CACHE).get();
        try {
            long size = tcf.size(file).get(5, TimeUnit.SECONDS);
            assertEquals(42, size);
            // NO_CACHE: no chunk memory should be allocated
            assertEquals(0, tcf.getGlobalCommittedBytes());
        } finally {
            tcf.close(file).get(5, TimeUnit.SECONDS);
        }
    }

    // =========================================================================
    // J. TransferTo branches
    // =========================================================================

    @Test
    public void testTransferToDirectReadPath() throws Exception {
        // transferPreferCache=false → preferCacheRead returns (false, true) for flushed data → delegate path
        TailCacheFileSystemConfig config = new TailCacheFileSystemConfig();
        config.setPerFileCacheLimits(10 * 1024, 1, CHUNK_SIZE);
        config.setMaxCacheSizeBytes(100 * 1024);
        config.setWriteBatchBytes(128);
        config.setIoWaitTimeoutMs(5000);
        config.setTransferPreferCache(false);
        TailCacheFileSystem tcfDirect = newTcf(config);

        String p = path("file_xfer_direct");
        writeFileSync(p, new byte[]{1, 2, 3, 4, 5});
        AsyncFile reader = tcfDirect.open(p, AbstractStorageFile.OpenMode.READ, false, false, null).get();
        try {
            delegate.reset();
            delegate.transferToCount = 0;
            AsyncTFSBasedFileSystemTest.ByteArrayOutputStreamChannel target =
                    new AsyncTFSBasedFileSystemTest.ByteArrayOutputStreamChannel();
            long n = tcfDirect.transferTo(reader, 0, 5, target).get(5, TimeUnit.SECONDS);
            assertEquals(5, n);
            assertTrue("transferPreferCache=false should use delegate", delegate.transferToCount > 0);
        } finally {
            tcfDirect.close(reader).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testTransferToCachePath() throws Exception {
        // transferPreferCache=true + data in cache → cache path (no delegate)
        String p = path("file_xfer_cache");
        AsyncFile writer = tcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        writeTcfSync(writer, new byte[]{10, 20, 30, 40, 50});
        tcf.fsync(writer).get(5, TimeUnit.SECONDS);
        // Writer has data in cache, transferPreferCache=true (default)
        delegate.reset();
        delegate.transferToCount = 0;
        AsyncTFSBasedFileSystemTest.ByteArrayOutputStreamChannel target =
                new AsyncTFSBasedFileSystemTest.ByteArrayOutputStreamChannel();
        long n = tcf.transferTo(writer, 1, 3, target).get(5, TimeUnit.SECONDS);
        assertEquals(3, n);
        assertArrayEquals(new byte[]{20, 30, 40}, target.toByteArray());
        assertEquals("cache path should not call delegate transferTo", 0, delegate.transferToCount);
        tcf.close(writer).get(5, TimeUnit.SECONDS);
    }

    // =========================================================================
    // K. Segment special branches
    // =========================================================================

    @Test
    public void testGetCurrentIndexFilesEmptyStateReturnsEmpty() throws Exception {
        // Empty dir state short-circuits to (0, empty map) before any delegate call. The delegate
        // itself has no such guard — it would hand back a handle per prefix keyed on offset 0.
        String dir = path("seg_idx_empty_state");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile reader = tcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, false, null).get();
        try {
            assertTrue(tcf.list(reader).isEmpty());
            Pair<Long, Map<String, AsyncFile>> result =
                    tcf.getCurrentIndexFiles(reader, INDEX_PREFIXES).get(5, TimeUnit.SECONDS);
            assertEquals(Long.valueOf(0), result.getKey());
            assertTrue("empty state must not open index handles", result.getValue().isEmpty());
            assertFalse(Files.exists(Paths.get(dir, IDX_PREFIX + "0")));
        } finally {
            tcf.close(reader).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testGetCurrentIndexFilesWriterReturnsTailSegmentHandles() throws Exception {
        // Non-empty state: the returned offset is the writer's tail segment and the handle map has
        // one entry per requested prefix.
        String dir = path("seg_idx_writer_tail");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile writer = tcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        try {
            tcf.write(writer, bufOf(new byte[10])).get(5, TimeUnit.SECONDS);
            tcf.roll(writer).get(5, TimeUnit.SECONDS);
            tcf.write(writer, bufOf(new byte[20])).get(5, TimeUnit.SECONDS);
            awaitAll();

            Pair<Long, Map<String, AsyncFile>> result =
                    tcf.getCurrentIndexFiles(writer, INDEX_PREFIXES).get(5, TimeUnit.SECONDS);
            assertEquals("keyed on the tail segment, not the first one",
                    Long.valueOf(10), result.getKey());
            assertEquals(tcf.getCurrentSegmentStartOffset(writer), (long) result.getKey());
            assertEquals(1, result.getValue().size());
            AsyncFile idx = result.getValue().get(IDX_PREFIX);
            assertNotNull(idx);
            assertTrue(Files.exists(Paths.get(dir, IDX_PREFIX + "10")));
        } finally {
            tcf.close(writer).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testDeleteSegmentsEmptyList() throws Exception {
        // deleteSegments with empty list → returns immediately, no-op
        String dir = path("seg_del_empty");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = tcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        tcf.write(seg, bufOf(new byte[10])).get(5, TimeUnit.SECONDS);
        tcf.roll(seg).get(5, TimeUnit.SECONDS);
        tcf.write(seg, bufOf(new byte[20])).get(5, TimeUnit.SECONDS);

        delegate.reset();
        tcf.deleteSegments(seg, Collections.emptyList()).get(5, TimeUnit.SECONDS);
        // No segments deleted
        List<Long> offsets = tcf.list(seg);
        assertEquals(2, offsets.size());
        tcf.close(seg).get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testDeleteSegmentsCacheDropBefore() throws Exception {
        // deleteSegments: when cacheStart < newFirstOffset → dropCacheBefore
        String dir = path("seg_del_cache");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile writer = tcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        tcf.write(writer, bufOf(new byte[10])).get(5, TimeUnit.SECONDS);
        tcf.roll(writer).get(5, TimeUnit.SECONDS);
        tcf.write(writer, bufOf(new byte[80])).get(5, TimeUnit.SECONDS);
        tcf.fsync(writer).get(5, TimeUnit.SECONDS);
        tcf.close(writer).get(5, TimeUnit.SECONDS);

        // Reopen writer — cache initialized at endOffset=90
        AsyncSegmentFile seg = tcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        FileCacheEntry entry = seg.getCacheEntry();
        assertTrue(entry.isInitialized());
        assertEquals(90, entry.cacheEndOffset);

        // Delete first segment (offset 0) → newFirstOffset = 10
        tcf.deleteSegments(seg, Collections.singletonList(0L)).get(5, TimeUnit.SECONDS);
        List<Long> offsets = tcf.list(seg);
        assertEquals(1, offsets.size());
        assertEquals(Long.valueOf(10), offsets.get(0));
        // Verify data still readable after cache drop
        tcf.close(seg).get(5, TimeUnit.SECONDS);

        AsyncSegmentFile reader = tcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, false, null).get();
        try {
            ByteBuf buf = tcf.read(reader, 80, 10).get(5, TimeUnit.SECONDS);
            assertEquals(80, buf.readableBytes());
            buf.release();
        } finally {
            tcf.close(reader).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSegmentDeleteClearsCache() throws Exception {
        // delete(AsyncSegmentFile): initialized cache → entry.clear()
        String dir = path("seg_del_clear");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = tcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        tcf.write(seg, bufOf(new byte[50])).get(5, TimeUnit.SECONDS);
        tcf.fsync(seg).get(5, TimeUnit.SECONDS);
        FileCacheEntry entry = seg.getCacheEntry();
        assertTrue(entry.isInitialized());

        tcf.delete(seg).get(5, TimeUnit.SECONDS);
        // After delete, cache should be cleared
        assertEquals(0, entry.cacheEndOffset);
        assertEquals(0, entry.cacheStartOffset);
    }

    // =========================================================================
    // L. Atomic cache & misc branches
    // =========================================================================

    @Test
    public void testAtomicReplaceCacheGenerationTracking() throws Exception {
        // Verify cacheGen increments on each atomic write, writtenGen tracks flush
        String p = path("file_atomic_gen");
        writeFileSync(p, new byte[]{1, 2, 3});
        AsyncFile writer = tcf.open(p, AbstractStorageFile.OpenMode.WRITE, true, false, null).get();
        FileCacheEntry entry = writer.getCacheEntry();
        assertTrue(entry.isInitialized());
        long gen0 = entry.cacheGen;

        // First atomic write → cacheGen should increment
        writeTcfSync(writer, new byte[]{10, 20, 30, 40});
        assertTrue("cacheGen should increment after write", entry.cacheGen > gen0);
        long gen1 = entry.cacheGen;

        // fsync → writtenGen should catch up to cacheGen
        tcf.fsync(writer).get(5, TimeUnit.SECONDS);
        assertEquals("writtenGen should match cacheGen after fsync", entry.cacheGen, entry.writtenGen);

        // Second atomic write → cacheGen increments again
        writeTcfSync(writer, new byte[]{50, 60});
        assertTrue("cacheGen should increment again", entry.cacheGen > gen1);

        tcf.close(writer).get(5, TimeUnit.SECONDS);
        assertArrayEquals(new byte[]{50, 60}, readFileSync(p));
    }

    @Test
    public void testResolveFileCacheModeOverrideTailCache() throws Exception {
        // Explicit TAIL_CACHE override with non-atomic → uses TAIL_CACHE
        String p = path("file_override_tc");
        AsyncFile writer = tcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null,
                AbstractStorageFile.CacheMode.TAIL_CACHE).get();
        assertEquals(AbstractStorageFile.CacheMode.TAIL_CACHE, writer.cacheMode);
        tcf.close(writer).get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testResolveFileCacheModeOverrideFullCache() throws Exception {
        // Explicit FULL_CACHE override → uses FULL_CACHE, WRITE upgrades to READ_WRITE
        String p = path("file_override_fc");
        writeFileSync(p, new byte[]{1, 2, 3});
        AsyncFile writer = tcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null,
                AbstractStorageFile.CacheMode.FULL_CACHE).get();
        assertEquals(AbstractStorageFile.CacheMode.FULL_CACHE, writer.cacheMode);
        // FULL_CACHE with WRITE → effectiveOpenMode upgraded to READ_WRITE
        // Verify: writer can both read existing data and append
        ByteBuf buf = tcf.read(writer, 3, 0).get(5, TimeUnit.SECONDS);
        try {
            assertArrayEquals(new byte[]{1, 2, 3}, readBytes(buf));
        } finally {
            writeTcfSync(writer, new byte[]{4, 5});
            tcf.close(writer).get(5, TimeUnit.SECONDS);
        }
        assertArrayEquals(new byte[]{1, 2, 3, 4, 5}, readFileSync(p));
    }

    @Test(expected = Exception.class)
    public void testResolveFileCacheModeAtomicTailCacheThrows() throws Exception {
        // atomicReplace + TAIL_CACHE override → IllegalArgumentException
        String p = path("file_atomic_tc_err");
        tcf.open(p, AbstractStorageFile.OpenMode.WRITE, true, false, null,
                AbstractStorageFile.CacheMode.TAIL_CACHE).get();
    }

    @Test(expected = Exception.class)
    public void testResolveSegmentCacheModeFullCacheThrows() throws Exception {
        // Segment + FULL_CACHE override → IllegalArgumentException
        String dir = path("seg_fc_err");
        Files.createDirectories(Paths.get(dir));
        tcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null,
                AbstractStorageFile.CacheMode.FULL_CACHE).get();
    }

    @Test
    public void testPositionAsyncFileWriteModeThrows() throws Exception {
        // The read-mode guard is a caller bug, so position(AsyncFile) throws it synchronously
        // rather than wrapping it in a failed future.
        String p = path("file_pos_write");
        AsyncFile writer = tcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        try {
            tcf.position(writer, 0);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("requires read mode"));
        } finally {
            tcf.close(writer).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testFullCacheWriteAndRead() throws Exception {
        // FULL_CACHE mode: entire file loaded, reads come from cache
        String p = path("file_full_rw");
        writeFileSync(p, new byte[]{1, 2, 3, 4, 5});
        AsyncFile writer = tcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null,
                AbstractStorageFile.CacheMode.FULL_CACHE).get();
        try {
            // FULL_CACHE loads entire file; writer should have all chunks
            FileCacheEntry entry = writer.getCacheEntry();
            assertTrue(entry.isInitialized());

            // Read existing data from cache
            delegate.reset();
            delegate.fileReadCount = 0;
            ByteBuf buf = tcf.read(writer, 5, 0).get(5, TimeUnit.SECONDS);
            try {
                assertArrayEquals(new byte[]{1, 2, 3, 4, 5}, readBytes(buf));
            } finally {
                // Append new data
                writeTcfSync(writer, new byte[]{6, 7, 8});
            }
            tcf.close(writer).get(5, TimeUnit.SECONDS);
        } finally {
            // already closed
        }
        assertArrayEquals(new byte[]{1, 2, 3, 4, 5, 6, 7, 8}, readFileSync(p));
    }

    @Test
    public void testTruncateSegmentInRangeReleasesLeases() throws Exception {
        // SegmentFileCacheEntry.truncateTo: offset in [cacheStart, cacheEnd) → truncateTo + releaseWriterIndexLeasesAfter
        String dir = path("seg_trunc_lease");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = tcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        tcf.write(seg, bufOf(new byte[10])).get(5, TimeUnit.SECONDS);
        tcf.roll(seg).get(5, TimeUnit.SECONDS);
        tcf.write(seg, bufOf(new byte[20])).get(5, TimeUnit.SECONDS);
        tcf.fsync(seg).get(5, TimeUnit.SECONDS);

        // Close + reopen to get initialized cache
        tcf.close(seg).get(5, TimeUnit.SECONDS);
        AsyncSegmentFile seg2 = tcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        FileCacheEntry entry = seg2.getCacheEntry();
        assertTrue(entry.isInitialized());
        assertEquals(30, entry.cacheEndOffset);

        // Truncate to 15 (inside cache range) → truncateTo + releaseWriterIndexLeasesAfter(15)
        tcf.truncate(seg2, 15).get(5, TimeUnit.SECONDS);
        assertEquals(15, entry.cacheEndOffset);
        tcf.close(seg2).get(5, TimeUnit.SECONDS);

        // Verify data on disk — segment read returns data from one segment at a time
        AsyncSegmentFile reader = tcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, false, null).get();
        try {
            // First segment [0, 10): 10 bytes
            ByteBuf buf = tcf.read(reader, 10, 0).get(5, TimeUnit.SECONDS);
            assertEquals(10, buf.readableBytes());
            buf.release();
            // Second segment [10, 15): 5 bytes
            ByteBuf buf2 = tcf.read(reader, 5, 10).get(5, TimeUnit.SECONDS);
            assertEquals(5, buf2.readableBytes());
            buf2.release();
        } finally {
            tcf.close(reader).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testDeleteSegmentsUsesLastDeletedOffset() throws Exception {
        String dir = path("seg_del_last_offset");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = tcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        tcf.write(seg, bufOf(new byte[10])).get(5, TimeUnit.SECONDS);
        tcf.roll(seg).get(5, TimeUnit.SECONDS);
        tcf.write(seg, bufOf(new byte[20])).get(5, TimeUnit.SECONDS);
        tcf.roll(seg).get(5, TimeUnit.SECONDS);
        tcf.write(seg, bufOf(new byte[5])).get(5, TimeUnit.SECONDS);

        // deleteSegments still requires the offsets to start at the first segment; only the
        // last one is used as the inclusive boundary by the underlying metadata call.
        try {
            tcf.deleteSegments(seg, Collections.singletonList(10L));
            fail("Expected IllegalArgumentException for out-of-order delete");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("expected 0"));
        }
        assertEquals(Arrays.asList(0L, 10L, 30L), tcf.list(seg));

        // Passing the full prefix deletes everything up to and including the last offset.
        tcf.deleteSegments(seg, Arrays.asList(0L, 10L)).get(5, TimeUnit.SECONDS);
        awaitAll();
        assertEquals(Collections.singletonList(30L), tcf.list(seg));
        assertFalse(Files.exists(Paths.get(dir, SEG_PREFIX + "0")));
        assertFalse(Files.exists(Paths.get(dir, SEG_PREFIX + "10")));
        assertTrue(Files.exists(Paths.get(dir, SEG_PREFIX + "30")));
        tcf.close(seg).get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testDeleteSegmentsCannotDeleteLast() throws Exception {
        // deleteSegments trying to delete all segments → IllegalArgumentException (thrown synchronously)
        String dir = path("seg_del_last");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = tcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        tcf.write(seg, bufOf(new byte[10])).get(5, TimeUnit.SECONDS);

        // Try to delete the only segment
        try {
            tcf.deleteSegments(seg, Collections.singletonList(0L));
            fail("Expected IllegalArgumentException for deleting last segment");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("cannot delete the last"));
        } finally {
            tcf.close(seg).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSizeSegmentEmptyDir() throws Exception {
        // size(AsyncSegmentFile) on empty directory → returns 0
        String dir = path("seg_size_empty2");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = tcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, false, null).get();
        try {
            long size = tcf.size(seg).get(5, TimeUnit.SECONDS);
            assertEquals(0, size);
        } finally {
            tcf.close(seg).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testWriteOnClosedSegmentThrows() throws Exception {
        // write after close → IllegalStateException
        String dir = path("seg_write_closed");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = tcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        tcf.close(seg).get(5, TimeUnit.SECONDS);
        try {
            tcf.write(seg, bufOf(new byte[]{1})).get(5, TimeUnit.SECONDS);
            fail("Expected exception for write on closed segment");
        } catch (Exception e) {
            // expected — cacheClosed check
        }
    }

    // =========================================================================
    // M. Chunk-level cache state verification
    // =========================================================================

    @Test
    public void testChunkDataAfterWrite() throws Exception {
        // Verify actual chunk buffer contents after multi-chunk writes
        String p = path("file_chunk_data");
        AsyncFile writer = tcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        // Write 200 bytes — spans chunks [0,64), [64,128), [128,192), [192,200)
        byte[] data = new byte[200];
        for (int i = 0; i < 200; i++) data[i] = (byte) (i % 128);
        writeTcfSync(writer, data);

        FileCacheEntry entry = writer.getCacheEntry();
        // Verify chunk 0: bytes [0..63]
        CacheChunk chunk0 = entry.chunks.get(0L);
        assertNotNull("chunk 0 should exist", chunk0);
        byte[] buf0 = new byte[64];
        chunk0.buffer.getBytes(0, buf0);
        for (int i = 0; i < 64; i++) assertEquals("chunk0 byte " + i, data[i], buf0[i]);

        // Verify chunk 2: bytes [128..191]
        CacheChunk chunk2 = entry.chunks.get(2L);
        assertNotNull("chunk 2 should exist", chunk2);
        byte[] buf2 = new byte[64];
        chunk2.buffer.getBytes(0, buf2);
        for (int i = 0; i < 64; i++) assertEquals("chunk2 byte " + i, data[128 + i], buf2[i]);

        // Verify chunk 3: bytes [192..199] (partial, 8 bytes)
        CacheChunk chunk3 = entry.chunks.get(3L);
        assertNotNull("chunk 3 should exist", chunk3);
        byte[] buf3 = new byte[8];
        chunk3.buffer.getBytes(0, buf3);
        for (int i = 0; i < 8; i++) assertEquals("chunk3 byte " + i, data[192 + i], buf3[i]);

        assertEquals(4, entry.chunks.size());
        tcf.close(writer).get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testChunkDataAfterTruncate() throws Exception {
        // Verify chunk buffer contents are correct after truncation
        String p = path("file_chunk_trunc");
        AsyncFile writer = tcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        // Write 200 bytes (4 chunks)
        byte[] data = new byte[200];
        for (int i = 0; i < 200; i++) data[i] = (byte) (i % 128);
        writeTcfSync(writer, data);
        tcf.fsync(writer).get(5, TimeUnit.SECONDS);

        FileCacheEntry entry = writer.getCacheEntry();
        assertEquals(4, entry.chunks.size());

        // Truncate to 100 bytes — should keep chunks 0 and 1, drop chunks 2 and 3
        tcf.truncate(writer, 100).get(5, TimeUnit.SECONDS);
        assertEquals(2, entry.chunks.size());
        assertNotNull("chunk 0 should survive", entry.chunks.get(0L));
        assertNotNull("chunk 1 should survive", entry.chunks.get(1L));
        assertNull("chunk 2 should be dropped", entry.chunks.get(2L));
        assertNull("chunk 3 should be dropped", entry.chunks.get(3L));

        // Verify chunk 1 data is intact (bytes 64..99)
        CacheChunk chunk1 = entry.chunks.get(1L);
        byte[] buf1 = new byte[36]; // only bytes 64..99 are valid
        chunk1.buffer.getBytes(0, buf1);
        for (int i = 0; i < 36; i++) assertEquals("chunk1 byte " + i, data[64 + i], buf1[i]);

        tcf.close(writer).get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testWrittenToFsOffsetAfterFlush() throws Exception {
        // writtenToFsOffset tracks what has been flushed to delegate
        String p = path("file_wfs_offset");
        AsyncFile writer = tcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        FileCacheEntry entry = writer.getCacheEntry();

        // After init (empty file): writtenToFsOffset = 0
        assertEquals(0, entry.writtenToFsOffset);

        // Write 50 bytes — cache has data but not yet flushed
        writeTcfSync(writer, new byte[50]);
        assertEquals(50, entry.cacheEndOffset);
        // writtenToFsOffset may not have advanced yet (async)

        // fsync forces flush — after fsync, writtenToFsOffset should match cacheEndOffset
        tcf.fsync(writer).get(5, TimeUnit.SECONDS);
        assertEquals("writtenToFsOffset should equal cacheEndOffset after fsync",
                entry.cacheEndOffset, entry.writtenToFsOffset);
        assertEquals(50, entry.writtenToFsOffset);

        // Write 30 more bytes
        writeTcfSync(writer, new byte[30]);
        assertEquals(80, entry.cacheEndOffset);
        // writtenToFsOffset should still be 50 (new data not flushed)
        assertEquals(50, entry.writtenToFsOffset);

        tcf.close(writer).get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testPendingFsyncBytesPropagation() throws Exception {
        // pendingFsyncBytes on entry should reflect delegate's pendingFsyncBytes after fsync
        String p = path("file_pfsync");
        AsyncFile writer = tcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        FileCacheEntry entry = writer.getCacheEntry();

        writeTcfSync(writer, new byte[100]);
        // fsync flushes + fsyncs → delegate's pendingFsyncBytes reset
        tcf.fsync(writer).get(5, TimeUnit.SECONDS);
        assertEquals("entry pendingFsyncBytes should be 0 after fsync",
                0, entry.pendingFsyncBytes);

        // Write more — delegate's pendingFsyncBytes accumulate, but entry may lag
        writeTcfSync(writer, new byte[50]);
        // After close, everything is flushed
        tcf.close(writer).get(5, TimeUnit.SECONDS);

        // Verify disk data is complete
        byte[] onDisk = readFileSync(p);
        assertEquals(150, onDisk.length);
    }

    @Test
    public void testBodySizeBytesTracking() throws Exception {
        // bodySizeBytes should accurately track total chunk memory
        String p = path("file_body_size");
        AsyncFile writer = tcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        FileCacheEntry entry = writer.getCacheEntry();

        assertEquals(0, entry.bodySizeBytes);

        // Write 200 bytes → 4 chunks × 64 bytes = 256 bytes allocated
        writeTcfSync(writer, new byte[200]);
        assertEquals("bodySizeBytes should be 4 chunks × 64", 4 * CHUNK_SIZE, entry.bodySizeBytes);

        // Truncate to 100 → 2 chunks remain
        tcf.fsync(writer).get(5, TimeUnit.SECONDS);
        tcf.truncate(writer, 100).get(5, TimeUnit.SECONDS);
        assertEquals("bodySizeBytes should be 2 chunks × 64 after truncate", 2 * CHUNK_SIZE, entry.bodySizeBytes);

        // Truncate to 0 → all chunks gone
        tcf.truncate(writer, 0).get(5, TimeUnit.SECONDS);
        assertEquals(0, entry.bodySizeBytes);

        tcf.close(writer).get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testCacheStartOffsetAfterEviction() throws Exception {
        // Eviction via evictTailBeforeAppend should advance cacheStartOffset and drop old chunks
        TailCacheFileSystemConfig config = new TailCacheFileSystemConfig();
        config.setPerFileCacheLimits(10 * 1024, 1, CHUNK_SIZE); // minRetainChunks=1
        config.setMaxCacheSizeBytes(200); // tight: 3 chunks (192) exceed 200
        config.setWriteBatchBytes(1024);
        config.setIoWaitTimeoutMs(5000);
        config.setExpectedMinRetentionMs(0); // no retention delay
        config.setEvictScanIntervalMs(60_000);
        config.setWatermarkRatios(0.3, 0.5);
        config.setMaxEvictRatioPerWrite(0.5);
        TailCacheFileSystem tightTcf = newTcf(config);

        String p = path("file_cso_evict");
        AsyncFile file = tightTcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        FileCacheEntry entry = file.getCacheEntry();

        // Write chunk 0 [0,64) and fsync — makes it durable (evictable)
        tightTcf.write(file, bufOf(new byte[(int) CHUNK_SIZE])).get(5, TimeUnit.SECONDS);
        tightTcf.fsync(file).get(5, TimeUnit.SECONDS);
        assertNotNull("chunk 0 should exist", entry.chunks.get(0L));
        assertEquals(0, entry.cacheStartOffset);

        // Write chunk 1 [64,128) and fsync
        tightTcf.write(file, bufOf(new byte[(int) CHUNK_SIZE])).get(5, TimeUnit.SECONDS);
        tightTcf.fsync(file).get(5, TimeUnit.SECONDS);
        assertEquals(2, entry.chunks.size());

        // Write chunk 2 [128,192) and fsync — total 192 > 200*0.5=100, triggers eviction
        // evictTailBeforeAppend: maxEvictable=3-1=2, ratio=0.96>high → minEvict=1
        // chunk 0 (durable) gets evicted → cacheStartOffset advances to 64
        tightTcf.write(file, bufOf(new byte[(int) CHUNK_SIZE])).get(5, TimeUnit.SECONDS);
        tightTcf.fsync(file).get(5, TimeUnit.SECONDS);

        // Verify chunk 0 was evicted
        assertNull("chunk 0 should be evicted", entry.chunks.get(0L));
        assertNotNull("chunk 1 should survive", entry.chunks.get(1L));
        assertNotNull("chunk 2 should survive", entry.chunks.get(2L));
        assertEquals("cacheStartOffset should advance to 64", 64, entry.cacheStartOffset);
        assertEquals(2, entry.chunks.size());

        // bodySizeBytes should reflect only surviving chunks
        assertEquals(2 * CHUNK_SIZE, entry.bodySizeBytes);

        tightTcf.close(file).get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testIndexFilesCleanupOnSegmentDelete() throws Exception {
        // SegmentFileCacheEntry.indexFiles should be cleaned up after delete
        String dir = path("seg_idx_cleanup");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = tcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        tcf.write(seg, bufOf(new byte[10])).get(5, TimeUnit.SECONDS);
        tcf.roll(seg).get(5, TimeUnit.SECONDS);
        tcf.write(seg, bufOf(new byte[20])).get(5, TimeUnit.SECONDS);
        tcf.fsync(seg).get(5, TimeUnit.SECONDS);

        SegmentFileCacheEntry segEntry = (SegmentFileCacheEntry) seg.getCacheEntry();
        // After fsync, index files should have entries
        assertFalse("indexFiles should have entries after write+roll", segEntry.indexFiles.isEmpty());

        // Delete all — entry.clear() should clean up indexFiles
        tcf.delete(seg).get(5, TimeUnit.SECONDS);
        assertTrue("indexFiles should be empty after delete", segEntry.indexFiles.isEmpty());
    }

    @Test
    public void testWriterIndexLeasesCleanupOnClose() throws Exception {
        // writerIndexLeaseStarts should be cleaned up after close
        String dir = path("seg_lease_cleanup");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = tcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        tcf.write(seg, bufOf(new byte[10])).get(5, TimeUnit.SECONDS);
        tcf.roll(seg).get(5, TimeUnit.SECONDS);
        tcf.write(seg, bufOf(new byte[20])).get(5, TimeUnit.SECONDS);

        SegmentFileCacheEntry segEntry = (SegmentFileCacheEntry) seg.getCacheEntry();
        // Writer has index leases
        assertFalse("writerIndexLeaseStarts should have entries",
                segEntry.writerIndexLeaseStarts.isEmpty());

        // Close — releaseEntry should clean up leases
        tcf.close(seg).get(5, TimeUnit.SECONDS);
        assertTrue("writerIndexLeaseStarts should be empty after close",
                segEntry.writerIndexLeaseStarts.isEmpty());
    }

    @Test
    public void testGlobalCommittedMatchesBodySize() throws Exception {
        // memoryTracker.committedBytes() should match sum of all entries' bodySizeBytes
        String p1 = path("file_gcb1");
        String p2 = path("file_gcb2");
        AsyncFile w1 = tcf.open(p1, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        AsyncFile w2 = tcf.open(p2, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();

        writeTcfSync(w1, new byte[(int) CHUNK_SIZE]);
        writeTcfSync(w2, new byte[(int) CHUNK_SIZE]);

        FileCacheEntry e1 = w1.getCacheEntry();
        FileCacheEntry e2 = w2.getCacheEntry();
        long expectedTotal = e1.bodySizeBytes + e2.bodySizeBytes;
        assertEquals("global committed should equal sum of bodySizeBytes",
                expectedTotal, tcf.getGlobalCommittedBytes());

        tcf.close(w1).get(5, TimeUnit.SECONDS);
        tcf.close(w2).get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testSegmentCacheIncludesIndexMemory() throws Exception {
        // SegmentFileCacheEntry.cacheSizeBytes should include index file memory
        String dir = path("seg_cache_idx_mem");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = tcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        tcf.write(seg, bufOf(new byte[10])).get(5, TimeUnit.SECONDS);
        tcf.roll(seg).get(5, TimeUnit.SECONDS);
        tcf.write(seg, bufOf(new byte[20])).get(5, TimeUnit.SECONDS);
        tcf.fsync(seg).get(5, TimeUnit.SECONDS);

        SegmentFileCacheEntry segEntry = (SegmentFileCacheEntry) seg.getCacheEntry();
        // cacheSizeBytes includes segment chunks + index file chunks
        long segOnlyBytes = segEntry.bodySizeBytes;
        long totalBytes = segEntry.cacheSizeBytes();
        assertTrue("cacheSizeBytes should include index memory (total >= segment only)",
                totalBytes >= segOnlyBytes);

        tcf.close(seg).get(5, TimeUnit.SECONDS);
    }

    // =========================================================================
    // N. ASYNC write-path failure handling & config validation
    // =========================================================================

    @Test
    public void testWriteMergesIntoInFlightFlushWithoutExtraDelegateWrite() throws Exception {
        // With a flush already in flight, a tail-cache write on a consistent entry is folded into it
        // (data.release() + return) rather than waiting. The bytes are already in the cache, so the
        // in-flight write picks them up and nothing is lost — and no second delegate write happens.
        FaultyDelegate faulty = newFaultyDelegate();
        TailCacheFileSystem hangTcf = newTcf(faulty, hangConfig());
        String p = path("merge_inflight");
        AsyncFile writer = hangTcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        FileCacheEntry entry = writer.getCacheEntry();

        faulty.hangOn(Op.FILE_WRITE);
        hangTcf.write(writer, bufOf(new byte[200])).get(5, TimeUnit.SECONDS);
        int writesAfterFirst = faulty.fileWriteCount;

        // These land while the first flush is stuck, so they must merge instead of queueing.
        long start = System.nanoTime();
        hangTcf.write(writer, bufOf(new byte[200])).get(5, TimeUnit.SECONDS);
        hangTcf.write(writer, bufOf(new byte[200])).get(5, TimeUnit.SECONDS);
        long elapsedMs = (System.nanoTime() - start) / 1_000_000L;

        assertTrue("merged writes must not wait, took " + elapsedMs + "ms", elapsedMs < BOUNDED_MS);
        assertEquals("no additional delegate write while one is in flight",
                writesAfterFirst, faulty.fileWriteCount);
        assertEquals(600, entry.cacheEndOffset);
        assertFalse(entry.fsInconsistent);

        faulty.release();
        hangTcf.fsync(writer).get(5, TimeUnit.SECONDS);
        awaitAll();
        assertEquals("everything buffered gets flushed", 600, entry.writtenToFsOffset);
        assertEquals(600, readFileSync(p).length);
        hangTcf.close(writer).get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testNoSpaceIsStickyAndCloseStillCompletes() throws Exception {
        // ENOSPC is latched on the file by executeWithIoFailureHandling, so later operations fail
        // fast via throwIfNoSpace instead of retrying a write that cannot succeed. close then
        // deliberately skips the flush (noSpaceBeforeClose -> noFs) so the handle can still be
        // released.
        FaultyDelegate faulty = newFaultyDelegate();
        TailCacheFileSystem enospcTcf = newTcf(faulty, baseConfig());
        String p = path("enospc");
        AsyncFile writer = enospcTcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();

        faulty.failOn(new IOException("No space left on device"), Op.FILE_WRITE);
        // Exceeds writeBatchBytes so the flush is attempted and fails inside the io task.
        enospcTcf.write(writer, bufOf(new byte[200])).get(5, TimeUnit.SECONDS);
        awaitAll();
        assertNotNull("ENOSPC latched on the file", writer.noSpaceFailure);

        faulty.release();
        // Even with the disk healthy again the latch makes further writes fail fast.
        try {
            enospcTcf.write(writer, bufOf(new byte[200]));
            fail("expected the ENOSPC latch to reject further writes");
        } catch (RuntimeException expected) {
            assertTrue(StorageUtil.isNoSpace(writer.noSpaceFailure));
        }

        enospcTcf.close(writer).get(5, TimeUnit.SECONDS);
        assertTrue("close must still release the handle after ENOSPC", writer.closed);
    }

    @Test
    public void testUndurableCacheGrowthFailsReserveInsteadOfLosingData() throws Exception {
        // Nothing is flushed, so the durableLimit check refuses to evict; the cache therefore hits
        // maxCacheSizeBytes. reserve waits ioWaitTimeoutMs and then fails the write rather than
        // silently dropping a chunk.
        TailCacheFileSystem tightTcf = newTcf(evictConfig(1, 200, 0).setIoWaitTimeoutMs(50));
        String p = path("reserve_timeout");
        AsyncFile writer = tightTcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        try {
            FileCacheEntry entry = writer.getCacheEntry();
            // 3 chunks = 192B fits under 200B.
            for (int i = 0; i < 3; i++) {
                tightTcf.write(writer, bufOf(new byte[(int) CHUNK_SIZE])).get(5, TimeUnit.SECONDS);
            }
            assertEquals(3, entry.chunks.size());
            assertEquals(0, entry.writtenToFsOffset);

            try {
                tightTcf.write(writer, bufOf(new byte[(int) CHUNK_SIZE]));
                fail("expected CacheMemoryReserveException");
            } catch (CacheMemoryReserveException expected) {
                // the write is rejected; the already-cached chunks are untouched
            }
            assertEquals("no chunk was sacrificed", 3, entry.chunks.size());
            assertEquals(3 * CHUNK_SIZE, entry.cacheEndOffset);
        } finally {
            tightTcf.close(writer).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testCloseChannelsFallBackWhenIoExecutorRejects() throws Exception {
        // scheduleCloseChannels retries on a dedicated close executor when the io executor refuses
        // the task, so a shut-down io pool cannot leak a channel detached by position().
        String dir = path("reject_close");
        Files.createDirectories(Paths.get(dir));

        // Build two segments first; moving a reader from the first to the second will detach its
        // currently-open FileChannel and hand it to scheduleCloseChannels.
        AsyncSegmentFile writer = tcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        tcf.write(writer, bufOf(new byte[10])).get(5, TimeUnit.SECONDS);
        tcf.roll(writer).get(5, TimeUnit.SECONDS);
        tcf.write(writer, bufOf(new byte[10])).get(5, TimeUnit.SECONDS);
        tcf.fsync(writer).get(5, TimeUnit.SECONDS);
        tcf.close(writer).get(5, TimeUnit.SECONDS);

        TrackingExecutor rejectIo = new TrackingExecutor(Executors.newCachedThreadPool());
        RecordingDelegate rejectingDelegate = new RecordingDelegate(rejectIo);
        TailCacheFileSystem rejectTcf = new TailCacheFileSystem(rejectingDelegate, baseConfig(), rejectIo);
        extraFileSystems.add(rejectTcf);

        AsyncSegmentFile reader = rejectTcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, false, null).get();
        try {
            // Open the first segment channel while the executor still accepts work.
            ByteBuf firstByte = rejectTcf.read(reader, 1, 0).get(5, TimeUnit.SECONDS);
            firstByte.release();
            FileChannel detached = reader.currentSegmentChannel;
            assertNotNull(detached);
            assertTrue(detached.isOpen());

            // position() itself is synchronous after the completed read barrier. Its close task is
            // rejected by rejectIo and must therefore be resubmitted to the private closeExecutor.
            rejectIo.shutdown();
            rejectTcf.position(reader, 10).get(5, TimeUnit.SECONDS);

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (detached.isOpen() && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            assertFalse("fallback close executor must close the detached channel", detached.isOpen());
        } finally {
            rejectTcf.close(reader).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testConfigValidatesTimeouts() {
        TailCacheFileSystemConfig config = new TailCacheFileSystemConfig();
        // ioWaitTimeoutMs must be positive: 0 used to be accepted and now is not.
        try {
            config.setIoWaitTimeoutMs(0);
            fail("expected IllegalArgumentException for ioWaitTimeoutMs=0");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("ioWaitTimeoutMs"));
        }
        try {
            config.setIoWaitTimeoutMs(-1);
            fail("expected IllegalArgumentException for a negative ioWaitTimeoutMs");
        } catch (IllegalArgumentException expected) {
            // expected
        }
        config.setIoWaitTimeoutMs(1);
        assertEquals(1, config.getIoWaitTimeoutMs());

        // restoreWaitTimeoutMs is also strictly positive.
        try {
            config.setRestoreWaitTimeoutMs(-1);
            fail("expected IllegalArgumentException for a negative restoreWaitTimeoutMs");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("restoreWaitTimeoutMs"));
        }
        try {
            config.setRestoreWaitTimeoutMs(0);
            fail("expected IllegalArgumentException for restoreWaitTimeoutMs=0");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("restoreWaitTimeoutMs"));
        }
        config.setRestoreWaitTimeoutMs(1);
        assertEquals(1, config.getRestoreWaitTimeoutMs());
        config.setRestoreWaitTimeoutMs(20_000);
        assertEquals(20_000, config.getRestoreWaitTimeoutMs());
    }

    // =========================================================================
    // O. EIO recovery
    // =========================================================================

    /**
     * EIO-capable delegate. When eioMode is enabled, writeSync throws EIOException.
     * fsyncIntervalBytes/fsyncIntervalMillis are set large so writeAndFlush never
     * auto-calls FileChannel.force() — data reaches page cache via writeSync but
     * is only persisted by explicit fsyncSync (triggered by tcf.fsync()).
     */
    static class EioDelegate extends AsyncTFSBasedFileSystem {
        volatile boolean eioMode = false;

        EioDelegate(ExecutorService ioExecutor) {
            super(ioExecutor, Long.MAX_VALUE / 2, Long.MAX_VALUE / 2_000_000L);
        }

        @Override
        public long writeSync(AsyncFile file, ByteBuf data) {
            if (eioMode) {
                throw new EIOException(new IOException("Input/output error"));
            }
            return super.writeSync(file, data);
        }
    }

    @Test
    public void testEioRecoveryWithPartialWriteAndCachePreservation() throws Exception {
        // Simulates: data reaches page cache via writeSync → lower-level fs EIO causes
        // partial data rollback → resetWrittenToFsOffset rolls back offset → cache preserved
        // → subsequent fsync re-flushes from cache and recovers.
        //
        // writeBatchBytes=1: every write triggers delegate.writeSync → data reaches page cache.
        // truncate(8): simulates partial write — offsets 0-7 survive, 8-9 lost.
        // write(C) with EIO: writeSync fails → resetWrittenToFsOffset via sizeSync → offset rolls back.
        // C's data still enters cache (initCacheAndAppend before writeSync).
        // fsync recovery: re-flushes B+C from cache (offsets 8-14).

        EioDelegate eioDelegate = new EioDelegate(ioExecutor);
        TailCacheFileSystem eioTcf = newTcf(eioDelegate,
                new TailCacheFileSystemConfig()
                        .setPerFileCacheLimits(10 * 1024, 1, CHUNK_SIZE)
                        .setMaxCacheSizeBytes(100 * 1024)
                        .setWriteBatchBytes(1) // every write goes to delegate (page cache)
                        .setIoWaitTimeoutMs(5000)
                        .setExpectedMinRetentionMs(0)
                        .setEvictScanIntervalMs(60_000)
                        .setWatermarkRatios(0.5, 0.8)
                        .setMaxEvictRatioPerWrite(0.5));

        String p = path("file_eio_recovery");
        AsyncFile writer = eioTcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        FileCacheEntry entry = writer.getCacheEntry();

        // Step 1: Write A={1,2,3,4,5} — writeSync → page cache
        eioTcf.write(writer, bufOf(new byte[]{1, 2, 3, 4, 5})).get();
        awaitAll();

        // Step 2: fsync — force page cache to disk, A durable
        eioTcf.fsync(writer).get();
        assertEquals(5, entry.cacheEndOffset);
        assertEquals(5, entry.writtenToFsOffset);

        // Step 3: Write B={6,7,8,9,10} — writeSync → page cache, NOT forced
        eioTcf.write(writer, bufOf(new byte[]{6, 7, 8, 9, 10})).get();
        awaitAll();
        assertEquals(10, entry.cacheEndOffset);
        assertEquals(10, entry.writtenToFsOffset);

        // Step 4: Truncate real file to 8 — simulates partial write (offsets 8,9 lost)
        try (FileChannel ch = FileChannel.open(Paths.get(p),
                java.nio.file.StandardOpenOption.WRITE)) {
            ch.truncate(8);
        }

        // Step 5: Write C={11,12,13,14,15} with EIO enabled
        // writeSync(C) → EIOException → executeWithEioRetry: reopen + resetWrittenToFsOffset
        // sizeSync returns 8 (truncate'd size) → writtenToFsOffset = 8
        // C's data still enters cache (initCacheAndAppend runs before writeSync)
        eioDelegate.eioMode = true;
        eioTcf.write(writer, bufOf(new byte[]{11, 12, 13, 14, 15})).get();
        awaitAll();
        assertEquals("cache preserved after EIO", 15, entry.cacheEndOffset);
        assertNotNull("chunk survives EIO", entry.chunks.get(0L));

        // Step 6: Recovery — fsync re-flushes from cache (offsets 8-14 = {9,10,11,12,13,14,15})
        eioDelegate.eioMode = false;
        eioTcf.fsync(writer).get();

        // Verify: all 15 bytes on disk with correct content
        byte[] onDisk = readFileSync(p);
        assertArrayEquals("disk should have {1..15} after recovery",
                new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, onDisk);

        eioTcf.close(writer).get();
    }

    // =========================================================================
    // FaultyDelegate — a RecordingDelegate whose chosen operations can be made to hang forever
    // (simulating a wedged disk, where IO never returns) or to fail with a given IOException.
    // =========================================================================

    enum Op { FILE_WRITE, FILE_FSYNC, FILE_SIZE, FILE_TRUNCATE, SEG_WRITE, MKDIR, ORPHAN_SCAN }

    static class FaultyDelegate extends RecordingDelegate {
        private volatile Set<Op> faulty = Collections.emptySet();
        private volatile CountDownLatch gate;
        private volatile IOException failure;

        FaultyDelegate(ExecutorService ioExecutor) {
            super(ioExecutor);
        }

        /** The given ops block until {@link #release()}; models a disk that stopped responding. */
        void hangOn(Op... ops) {
            gate = new CountDownLatch(1);
            failure = null;
            faulty = new HashSet<>(Arrays.asList(ops));
        }

        void failOn(IOException e, Op... ops) {
            gate = null;
            failure = e;
            faulty = new HashSet<>(Arrays.asList(ops));
        }

        void release() {
            CountDownLatch g = gate;
            faulty = Collections.emptySet();
            failure = null;
            gate = null;
            if (g != null) {
                g.countDown();
            }
        }

        private void fault(Op op) {
            if (!faulty.contains(op)) {
                return;
            }
            IOException f = failure;
            if (f != null) {
                throw StorageUtil.wrapIOException(f);
            }
            CountDownLatch g = gate;
            if (g == null) {
                return;
            }
            try {
                // Bounded so a forgotten release() fails the test instead of wedging the build.
                if (!g.await(30, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("faulty gate never released for " + op);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }

        // Release the buffer the real writeSync would have released, so an injected failure does
        // not look like a leak.
        private void faultWrite(Op op, ByteBuf data) {
            try {
                fault(op);
            } catch (RuntimeException e) {
                data.release();
                throw e;
            }
        }

        @Override
        public long writeSync(AsyncFile file, ByteBuf data) {
            faultWrite(Op.FILE_WRITE, data);
            return super.writeSync(file, data);
        }

        @Override
        public long writeSync(AsyncSegmentFile file, ByteBuf data) {
            faultWrite(Op.SEG_WRITE, data);
            return super.writeSync(file, data);
        }

        @Override
        public void fsyncSync(AsyncFile file) {
            fault(Op.FILE_FSYNC);
            super.fsyncSync(file);
        }

        @Override
        public long sizeSync(AsyncFile file) {
            fault(Op.FILE_SIZE);
            return super.sizeSync(file);
        }

        @Override
        public void truncateSync(AsyncFile file, long size) {
            fault(Op.FILE_TRUNCATE);
            super.truncateSync(file, size);
        }

        @Override
        public boolean mkdirSync(String path, boolean recursive) {
            fault(Op.MKDIR);
            return super.mkdirSync(path, recursive);
        }

        @Override
        public void deleteOrphanSegmentFilesSync(AsyncSegmentFile file) {
            fault(Op.ORPHAN_SCAN);
            super.deleteOrphanSegmentFilesSync(file);
        }
    }

    private FaultyDelegate newFaultyDelegate() {
        FaultyDelegate created = new FaultyDelegate(ioExecutor);
        faultyDelegates.add(created);
        return created;
    }

    // =========================================================================
    // RecordingDelegate — wraps AsyncTFSBasedFileSystem to count delegate calls
    // =========================================================================

    static class RecordingDelegate extends AsyncTFSBasedFileSystem {
        int fileWriteCount;
        int fileReadCount;
        int fileFsyncCount;
        int fileCloseCount;
        int segWriteCount;
        int segReadCount;
        int segFsyncCount;
        int segRollCount;
        int transferToCount;
        final List<byte[]> fileWrittenData = new ArrayList<>();
        final List<byte[]> segWrittenData = new ArrayList<>();

        RecordingDelegate(ExecutorService ioExecutor) {
            super(ioExecutor, Long.MAX_VALUE / 2, Long.MAX_VALUE / 2_000_000L);
        }

        void reset() {
            fileWriteCount = fileReadCount = fileFsyncCount = fileCloseCount = 0;
            segWriteCount = segReadCount = segFsyncCount = segRollCount = 0;
            transferToCount = 0;
            fileWrittenData.clear();
            segWrittenData.clear();
        }

        // ---- AsyncFile ----

        @Override
        public long writeSync(AsyncFile file, ByteBuf data) {
            fileWriteCount++;
            byte[] copy = new byte[data.readableBytes()];
            data.getBytes(data.readerIndex(), copy);
            fileWrittenData.add(copy);
            return super.writeSync(file, data);
        }

        @Override
        public ByteBuf readSync(AsyncFile file, long length, long offset, long alignSize) {
            fileReadCount++;
            return super.readSync(file, length, offset, alignSize);
        }

        @Override
        public void fsyncSync(AsyncFile file) {
            fileFsyncCount++;
            super.fsyncSync(file);
        }

        @Override
        public List<FileChannel> closeSync(AsyncFile file) {
            fileCloseCount++;
            return super.closeSync(file);
        }

        @Override
        public long transferToSync(AsyncFile file, long position, long count, WritableByteChannel target) {
            transferToCount++;
            return super.transferToSync(file, position, count, target);
        }

        // ---- AsyncSegmentFile ----

        @Override
        public long writeSync(AsyncSegmentFile file, ByteBuf data) {
            segWriteCount++;
            byte[] copy = new byte[data.readableBytes()];
            data.getBytes(data.readerIndex(), copy);
            segWrittenData.add(copy);
            return super.writeSync(file, data);
        }

        @Override
        public ByteBuf readSync(AsyncSegmentFile file, long length, long offset) {
            segReadCount++;
            return super.readSync(file, length, offset);
        }

        @Override
        public void fsyncSync(AsyncSegmentFile file) {
            segFsyncCount++;
            super.fsyncSync(file);
        }

        @Override
        public List<FileChannel> rollMetadataSync(AsyncSegmentFile file, long currentSegmentSize, boolean noFs) {
            segRollCount++;
            return super.rollMetadataSync(file, currentSegmentSize, noFs);
        }

        @Override
        public long transferToSync(AsyncSegmentFile file, long offset, long count, WritableByteChannel target) {
            transferToCount++;
            return super.transferToSync(file, offset, count, target);
        }
    }
}
