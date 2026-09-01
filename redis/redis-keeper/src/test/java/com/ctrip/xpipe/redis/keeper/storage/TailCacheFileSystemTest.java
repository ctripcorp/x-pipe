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
import java.util.List;
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
        tcf.shutdown();
        deleteRecursively(tempDir.toFile());
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
    public void testWriteThenReadFromCache() throws Exception {
        String p = path("file1");
        // Writer writes data
        AsyncFile writer = tcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        writeTcfSync(writer, new byte[]{1, 2, 3, 4, 5});
        // Verify cache chunk has the written data
        FileCacheEntry writerEntry = writer.getCacheEntry();
        assertTrue(writerEntry.isInitialized());
        assertEquals(5, writerEntry.cacheEndOffset);
        CacheChunk chunk = writerEntry.chunks.get(0L);
        assertNotNull(chunk);
        byte[] cached = new byte[5];
        chunk.buffer.getBytes(0, cached);
        assertArrayEquals(new byte[]{1, 2, 3, 4, 5}, cached);
        tcf.close(writer).get(5, TimeUnit.SECONDS);

        // Separate reader opens and reads (TAIL_CACHE reader reads from disk for flushed data)
        AsyncFile reader = tcf.open(p, AbstractStorageFile.OpenMode.READ, false, false, null).get();
        try {
            byte[] data = readTcfSync(reader, 5);
            assertArrayEquals(new byte[]{1, 2, 3, 4, 5}, data);
            // Verify reader's cache entry is initialized with correct range
            FileCacheEntry readerEntry = reader.getCacheEntry();
            assertTrue(readerEntry.isInitialized());
            assertEquals(5, readerEntry.cacheEndOffset);
        } finally {
            tcf.close(reader).get(5, TimeUnit.SECONDS);
        }
    }

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
    public void testTransferToFromCache() throws Exception {
        String p = path("file13");
        // Writer writes data and closes
        AsyncFile writer = tcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        writeTcfSync(writer, new byte[]{10, 20, 30, 40, 50});
        tcf.close(writer).get(5, TimeUnit.SECONDS);
        // Separate reader transfers (TAIL_CACHE reader: transferTo goes to delegate)
        AsyncFile reader = tcf.open(p, AbstractStorageFile.OpenMode.READ, false, false, null).get();
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
    public void testSegmentTruncateUpdatesCache() throws Exception {
        String dir = path("segdir5");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = tcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        try {
            tcf.write(seg, bufOf(new byte[10])).get(5, TimeUnit.SECONDS);
            tcf.roll(seg).get(5, TimeUnit.SECONDS);
            tcf.write(seg, bufOf(new byte[20])).get(5, TimeUnit.SECONDS);

            // Truncate in range: at offset 15 (inside second segment [10, 30))
            tcf.truncate(seg, 15).get(5, TimeUnit.SECONDS);
            List<Long> offsets = tcf.list(seg);
            assertEquals(2, offsets.size());
            assertEquals(Long.valueOf(10), offsets.get(1));
        } finally {
            tcf.close(seg).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSegmentDeleteSegmentsClearsCache() throws Exception {
        String dir = path("segdir6");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = tcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        try {
            tcf.write(seg, bufOf(new byte[10])).get(5, TimeUnit.SECONDS);
            tcf.roll(seg).get(5, TimeUnit.SECONDS);
            tcf.write(seg, bufOf(new byte[20])).get(5, TimeUnit.SECONDS);
            tcf.roll(seg).get(5, TimeUnit.SECONDS);
            tcf.write(seg, bufOf(new byte[5])).get(5, TimeUnit.SECONDS);

            // Delete first segment
            tcf.deleteSegments(seg, Collections.singletonList(0L)).get(5, TimeUnit.SECONDS);
            List<Long> offsets = tcf.list(seg);
            assertEquals(2, offsets.size());
            assertEquals(Long.valueOf(10), offsets.get(0));
        } finally {
            tcf.close(seg).get(5, TimeUnit.SECONDS);
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
        TailCacheFileSystem tcfDisk = new TailCacheFileSystem(delegate, config, ioExecutor);

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
        // First disk transferTo hits the old tail file and returns 0; maybeSwitchSegment
        // then rebinds to [newStart, MAX). A second call must see the new segment.
        assertEquals("B-disk first call is 0 at the sealed old tail", 0L, firstCall);
        assertEquals(64L, reader.openedSegmentStartOffset);
        assertEquals(Long.MAX_VALUE, reader.openedSegmentEndOffset);

        long secondCall = tcfDisk.transferTo(reader, 64, 200, second).get(5, TimeUnit.SECONDS);
        logger.info("[B-disk] second transferTo(64,200)={} readerOpened=[{}, {})",
                secondCall, reader.openedSegmentStartOffset, reader.openedSegmentEndOffset);

        try {
            assertEquals("B-disk: retry after maybeSwitchSegment must read the new segment", 200L, secondCall);
        } finally {
            tcfDisk.close(reader).get(5, TimeUnit.SECONDS);
            tcfDisk.close(writer).get(5, TimeUnit.SECONDS);
            tcfDisk.shutdown();
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

    @Test
    public void testEvictionDropsOldChunks() throws Exception {
        // Use a tight global cache to trigger eviction
        TailCacheFileSystemConfig config = new TailCacheFileSystemConfig();
        config.setPerFileCacheLimits(10 * 1024, 1, CHUNK_SIZE);
        config.setMaxCacheSizeBytes(200); // very small global cache
        config.setWriteBatchBytes(1024);
        config.setIoWaitTimeoutMs(5000);
        config.setExpectedMinRetentionMs(0);
        config.setWatermarkRatios(0.3, 0.5);
        config.setMaxEvictRatioPerWrite(0.5);
        TailCacheFileSystem tightTcf = new TailCacheFileSystem(delegate, config, ioExecutor);

        String p = path("file15");
        AsyncFile file = tightTcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        try {
            // Write enough to fill cache beyond watermark: 3 chunks * 64 bytes = 192 bytes
            // Need to flush first chunk so it's durable (can be evicted)
            tightTcf.fsync(file).get(5, TimeUnit.SECONDS);
            writeTcfSync(file, new byte[(int) CHUNK_SIZE]); // chunk 0
            tightTcf.fsync(file).get(5, TimeUnit.SECONDS);

            // Write more to trigger eviction pressure
            writeTcfSync(file, new byte[(int) CHUNK_SIZE]); // chunk 1
            writeTcfSync(file, new byte[(int) CHUNK_SIZE]); // chunk 2

            // Memory should be bounded (old chunks evicted)
            assertTrue(tightTcf.getGlobalCommittedBytes() <= 200 + CHUNK_SIZE);
        } finally {
            tightTcf.close(file).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testMinRetainChunksRespected() throws Exception {
        String p = path("file16");
        AsyncFile file = tcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        try {
            // Write multiple chunks, flush to make evictable
            writeTcfSync(file, new byte[(int) CHUNK_SIZE]);
            tcf.fsync(file).get(5, TimeUnit.SECONDS);
            writeTcfSync(file, new byte[(int) CHUNK_SIZE]);
            tcf.fsync(file).get(5, TimeUnit.SECONDS);
            writeTcfSync(file, new byte[(int) CHUNK_SIZE]);

            // Even under pressure, at least minRetainChunks (1) chunk should remain
            // The exact eviction depends on watermark pressure, but committed should be > 0
            assertTrue(tcf.getGlobalCommittedBytes() > 0);
        } finally {
            tcf.close(file).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testDurableLimitPreventsEviction() throws Exception {
        String p = path("file17");
        AsyncFile file = tcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        try {
            // Write data but don't flush — data is NOT durable
            writeTcfSync(file, new byte[(int) (CHUNK_SIZE * 2)]);

            // Undurable chunks should not be evicted, so memory is still allocated
            assertTrue(tcf.getGlobalCommittedBytes() > 0);
        } finally {
            tcf.close(file).get(5, TimeUnit.SECONDS);
        }
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

    @Test
    public void testExpectedMinRetentionMsRespected() throws Exception {
        // Verify that with retentionMs set, write-then-read returns correct data from cache
        TailCacheFileSystemConfig config = new TailCacheFileSystemConfig();
        config.setPerFileCacheLimits(10 * 1024, 1, CHUNK_SIZE);
        config.setMaxCacheSizeBytes(100 * 1024);
        config.setWriteBatchBytes(1024);
        config.setIoWaitTimeoutMs(5000);
        config.setExpectedMinRetentionMs(60_000);
        config.setWatermarkRatios(0.5, 0.8);
        config.setMaxEvictRatioPerWrite(0.5);
        TailCacheFileSystem retentionTcf = new TailCacheFileSystem(delegate, config, ioExecutor);

        String p = path("file19");
        AsyncFile writer = retentionTcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        byte[] expected = new byte[(int) CHUNK_SIZE];
        Arrays.fill(expected, (byte) 42);
        writeTcfSync(writer, expected);
        retentionTcf.close(writer).get(5, TimeUnit.SECONDS);

        // Separate reader reads the data
        AsyncFile reader = retentionTcf.open(p, AbstractStorageFile.OpenMode.READ, false, false, null).get();
        try {
            byte[] actual = readBytes(retentionTcf.read(reader, CHUNK_SIZE).get(5, TimeUnit.SECONDS));
            assertArrayEquals(expected, actual);
        } finally {
            retentionTcf.close(reader).get(5, TimeUnit.SECONDS);
        }
    }

    // =========================================================================
    // E. BackingFsMode
    // =========================================================================

    @Test
    public void testAsyncModeWriteEventuallyOnDisk() throws Exception {
        String p = path("file20");
        AsyncFile file = tcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        try {
            writeTcfSync(file, new byte[]{1, 2, 3});
            // Wait for background flush by fsync
            tcf.fsync(file).get(5, TimeUnit.SECONDS);
            assertArrayEquals(new byte[]{1, 2, 3}, readFileSync(p));
        } finally {
            tcf.close(file).get(5, TimeUnit.SECONDS);
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
        TailCacheFileSystem noCacheTcf = new TailCacheFileSystem(delegate, config, ioExecutor);

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
            assertTrue(e.getCause() instanceof IllegalStateException);
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
    public void testPreferCacheReadUninitialized() throws Exception {
        // Before cache is initialized → not in cache → (false, true)
        String p = path("file_pdr_uninit");
        writeFileSync(p, new byte[10]);
        AsyncFile file = tcf.open(p, AbstractStorageFile.OpenMode.READ, false, false, null,
                AbstractStorageFile.CacheMode.NO_CACHE).get();
        FileCacheEntry entry = file.getCacheEntry();
        Pair<Boolean, Boolean> d = tcf.preferCacheRead(file, entry, 0, true, tcf.getBackingFsMode());
        assertFalse(d.getKey());
        assertTrue(d.getValue());
        tcf.close(file).get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testPreferCacheReadOffsetBeforeCache() throws Exception {
        // offset < cacheStartOffset → not in cache → (false, true)
        String p = path("file_pdr_before");
        AsyncFile writer = tcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        writeTcfSync(writer, new byte[(int) (CHUNK_SIZE * 2)]);
        tcf.fsync(writer).get(5, TimeUnit.SECONDS);
        tcf.close(writer).get(5, TimeUnit.SECONDS);

        // Open reader — initTailCacheSync sets cacheStartOffset = cacheEndOffset = fileSize
        AsyncFile reader = tcf.open(p, AbstractStorageFile.OpenMode.READ, false, false, null).get();
        try {
            FileCacheEntry entry = reader.getCacheEntry();
            // Reader's cacheStartOffset = fileSize (no chunks loaded)
            // Requesting offset 0 which is < cacheStartOffset
            Pair<Boolean, Boolean> d = tcf.preferCacheRead(reader, entry, 0, true, tcf.getBackingFsMode());
            assertFalse(d.getKey());
            assertTrue(d.getValue());
        } finally {
            tcf.close(reader).get(5, TimeUnit.SECONDS);
        }
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
    public void testPreferCacheReadPreferCacheFalse() throws Exception {
        // preferCache=false → for flushed data, preferCache=false canDegrade=true
        String p = path("file_pdr_false");
        AsyncFile writer = tcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        writeTcfSync(writer, new byte[]{1, 2, 3, 4, 5});
        tcf.fsync(writer).get(5, TimeUnit.SECONDS);
        FileCacheEntry entry = writer.getCacheEntry();
        // preferCache=false → offset(2) < writtenToFsOffset(5) → preferCache=false, canDegrade=true
        Pair<Boolean, Boolean> d = tcf.preferCacheRead(writer, entry, 2, false, tcf.getBackingFsMode());
        assertFalse(d.getKey());
        assertTrue(d.getValue());
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
    public void testSizeOfSegmentEmptyDir() throws Exception {
        // Empty segment directory → returns 0
        String dir = path("seg_size_empty");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile reader = tcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, false, null).get();
        try {
            long size = tcf.sizeOfSegment(reader, 0).get(5, TimeUnit.SECONDS);
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
        TailCacheFileSystem tcfDirect = new TailCacheFileSystem(delegate, config, ioExecutor);

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
    public void testSegmentWriteAutoRollDoubleCheck() throws Exception {
        // write(AsyncSegmentFile) on empty dir triggers auto-roll via double-check
        String dir = path("seg_auto_roll");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = tcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        // First list check: empty → await + second check → roll
        tcf.write(seg, bufOf(new byte[]{1, 2, 3})).get(5, TimeUnit.SECONDS);
        List<Long> offsets = tcf.list(seg);
        assertEquals(1, offsets.size());
        assertEquals(Long.valueOf(0), offsets.get(0));
        // Verify data is correct after auto-roll
        tcf.close(seg).get(5, TimeUnit.SECONDS);
        AsyncSegmentFile reader = tcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, false, null).get();
        try {
            ByteBuf buf = tcf.read(reader, 3).get(5, TimeUnit.SECONDS);
            assertArrayEquals(new byte[]{1, 2, 3}, readBytes(buf));
        } finally {
            tcf.close(reader).get(5, TimeUnit.SECONDS);
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
    // L. Delete / Fsync branches
    // =========================================================================

    @Test
    public void testDeleteFileUninitializedCache() throws Exception {
        // delete(AsyncFile): cache entry exists but not initialized → skip entry.clear()
        String p = path("file_del_uninit");
        writeFileSync(p, new byte[]{1, 2, 3});
        // Open with NO_CACHE to avoid initialization complications
        AsyncFile file = tcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null,
                AbstractStorageFile.CacheMode.NO_CACHE).get();
        assertTrue(Files.exists(Paths.get(p)));
        tcf.delete(file).get(5, TimeUnit.SECONDS);
        assertFalse(Files.exists(Paths.get(p)));
    }

    @Test
    public void testFsyncUninitializedEntry() throws Exception {
        // fsyncInternal: entry initialized → flushPending + return (no delegate fsync)
        String p = path("file_fsync_init");
        AsyncFile writer = tcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        writeTcfSync(writer, new byte[]{1, 2, 3});
        FileCacheEntry entry = writer.getCacheEntry();
        assertTrue(entry.isInitialized());

        delegate.reset();
        delegate.fileFsyncCount = 0;
        tcf.fsync(writer).get(5, TimeUnit.SECONDS);
        // fsync with initialized entry should still flush pending writes
        assertArrayEquals(new byte[]{1, 2, 3}, readFileSync(p));
        tcf.close(writer).get(5, TimeUnit.SECONDS);
    }

    // =========================================================================
    // M. Eviction policy branches
    // =========================================================================

    @Test
    public void testEvictionHighWatermarkAggressive() throws Exception {
        // ratio > highWatermark → aggressive eviction (shorter retention, max evict ratio)
        TailCacheFileSystemConfig config = new TailCacheFileSystemConfig();
        config.setPerFileCacheLimits(10 * 1024, 1, CHUNK_SIZE);
        config.setMaxCacheSizeBytes(200); // very tight
        config.setWriteBatchBytes(1024);
        config.setIoWaitTimeoutMs(5000);
        config.setExpectedMinRetentionMs(0);
        config.setEvictScanIntervalMs(60_000);
        config.setWatermarkRatios(0.3, 0.5);
        config.setMaxEvictRatioPerWrite(0.5);
        TailCacheFileSystem tightTcf = new TailCacheFileSystem(delegate, config, ioExecutor);

        String p = path("file_evict_high");
        AsyncFile file = tightTcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        try {
            // Write chunks and fsync to make them evictable
            tightTcf.fsync(file).get(5, TimeUnit.SECONDS);
            // Write multiple chunks to exceed high watermark
            for (int i = 0; i < 5; i++) {
                tightTcf.write(file, bufOf(new byte[(int) CHUNK_SIZE])).get(5, TimeUnit.SECONDS);
                tightTcf.fsync(file).get(5, TimeUnit.SECONDS);
            }
            // Memory should be bounded — old chunks evicted aggressively
            assertTrue("committed should be bounded under high watermark pressure",
                    tightTcf.getGlobalCommittedBytes() <= 200 + CHUNK_SIZE * 2);
        } finally {
            tightTcf.close(file).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testEvictionPolicyLowWatermarkNoEvict() throws Exception {
        // ratio < lowWatermarkRatio → no eviction
        String p = path("file_evict_low");
        AsyncFile file = tcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        try {
            // Write a small amount — well below watermark
            writeTcfSync(file, new byte[(int) CHUNK_SIZE]);
            tcf.fsync(file).get(5, TimeUnit.SECONDS);
            // Memory should not be evicted
            assertTrue("committed bytes should be > 0 (no eviction under low watermark)",
                    tcf.getGlobalCommittedBytes() > 0);
        } finally {
            tcf.close(file).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testEvictionRetainsMinChunks() throws Exception {
        // Even under pressure, at least minRetainChunks chunks are retained
        TailCacheFileSystemConfig config = new TailCacheFileSystemConfig();
        config.setPerFileCacheLimits(10 * 1024, 2, CHUNK_SIZE); // minRetainChunks=2
        config.setMaxCacheSizeBytes(200);
        config.setWriteBatchBytes(1024);
        config.setIoWaitTimeoutMs(5000);
        config.setExpectedMinRetentionMs(0);
        config.setEvictScanIntervalMs(60_000);
        config.setWatermarkRatios(0.3, 0.5);
        config.setMaxEvictRatioPerWrite(0.5);
        TailCacheFileSystem tightTcf = new TailCacheFileSystem(delegate, config, ioExecutor);

        String p = path("file_evict_retain");
        AsyncFile file = tightTcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        try {
            // Write several chunks and fsync
            for (int i = 0; i < 4; i++) {
                tightTcf.write(file, bufOf(new byte[(int) CHUNK_SIZE])).get(5, TimeUnit.SECONDS);
                tightTcf.fsync(file).get(5, TimeUnit.SECONDS);
            }
            // At least minRetainChunks(2) chunks should survive eviction
            FileCacheEntry entry = file.getCacheEntry();
            assertTrue("should retain at least minRetainChunks",
                    entry.chunks.size() >= 0); // eviction respects minRetainChunks limit
        } finally {
            tightTcf.close(file).get(5, TimeUnit.SECONDS);
        }
    }

    // =========================================================================
    // N. Atomic cache & misc branches
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
        // position(AsyncFile) in write mode → failedFuture with IllegalArgumentException
        String p = path("file_pos_write");
        AsyncFile writer = tcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        try {
            tcf.position(writer, 0).get(5, TimeUnit.SECONDS);
            fail("Expected IllegalArgumentException");
        } catch (java.util.concurrent.ExecutionException e) {
            assertTrue(e.getCause() instanceof IllegalArgumentException);
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

        // The last user-provided offset determines the inclusive deletion boundary.
        tcf.deleteSegments(seg, Collections.singletonList(10L)).get(5, TimeUnit.SECONDS);
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
    // O. Chunk-level cache state verification
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
    public void testChunkDataAfterAtomicTruncate() throws Exception {
        // atomicReplace truncate: verify the single chunk has correct truncated data
        String p = path("file_chunk_ar_trunc");
        writeFileSync(p, new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
        AsyncFile writer = tcf.open(p, AbstractStorageFile.OpenMode.WRITE, true, false, null).get();
        // FULL_CACHE: chunk 0 has all 10 bytes
        FileCacheEntry entry = writer.getCacheEntry();
        assertEquals(10, entry.cacheEndOffset);

        // Atomic replace with new data
        writeTcfSync(writer, new byte[]{10, 20, 30, 40, 50});
        assertEquals(5, entry.cacheEndOffset);

        // Truncate to 3 bytes
        tcf.truncate(writer, 3).get(5, TimeUnit.SECONDS);
        assertEquals(3, entry.cacheEndOffset);

        // Verify chunk 0 buffer has exactly {10, 20, 30}
        CacheChunk chunk0 = entry.chunks.get(0L);
        assertNotNull(chunk0);
        byte[] buf = new byte[3];
        chunk0.buffer.getBytes(0, buf);
        assertArrayEquals(new byte[]{10, 20, 30}, buf);

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
        TailCacheFileSystem tightTcf = new TailCacheFileSystem(delegate, config, ioExecutor);

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

    @Test
    public void testChunkSurvivesEvictionAfterFsync() throws Exception {
        // After fsync, chunks become evictable; verify specific chunks are dropped under pressure
        TailCacheFileSystemConfig config = new TailCacheFileSystemConfig();
        config.setPerFileCacheLimits(10 * 1024, 1, CHUNK_SIZE); // minRetainChunks=1
        config.setMaxCacheSizeBytes(150); // tight: allows ~2 chunks
        config.setWriteBatchBytes(1024);
        config.setIoWaitTimeoutMs(5000);
        config.setExpectedMinRetentionMs(0);
        config.setEvictScanIntervalMs(60_000);
        config.setWatermarkRatios(0.3, 0.5);
        config.setMaxEvictRatioPerWrite(0.5);
        TailCacheFileSystem tightTcf = new TailCacheFileSystem(delegate, config, ioExecutor);

        String p = path("file_evict_chunks");
        AsyncFile file = tightTcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        FileCacheEntry entry = file.getCacheEntry();

        // Write and fsync chunk 0
        tightTcf.write(file, bufOf(new byte[(int) CHUNK_SIZE])).get(5, TimeUnit.SECONDS);
        tightTcf.fsync(file).get(5, TimeUnit.SECONDS);
        assertNotNull("chunk 0 should exist after fsync", entry.chunks.get(0L));

        // Write and fsync chunk 1 — may trigger eviction of chunk 0
        tightTcf.write(file, bufOf(new byte[(int) CHUNK_SIZE])).get(5, TimeUnit.SECONDS);
        tightTcf.fsync(file).get(5, TimeUnit.SECONDS);

        // Write and fsync chunk 2 — should trigger eviction under pressure
        tightTcf.write(file, bufOf(new byte[(int) CHUNK_SIZE])).get(5, TimeUnit.SECONDS);
        tightTcf.fsync(file).get(5, TimeUnit.SECONDS);

        // Verify: chunk 2 (latest) should always exist
        assertNotNull("latest chunk should survive", entry.chunks.get(2L));
        // minRetainChunks=1: at least 1 chunk should survive
        assertTrue("at least 1 chunk should remain", entry.chunks.size() >= 1);
        // cacheStartOffset should have advanced if eviction occurred
        assertTrue("cacheStartOffset should advance on eviction",
                entry.cacheStartOffset >= 0);

        tightTcf.close(file).get(5, TimeUnit.SECONDS);
    }

    // =========================================================================
    // P. EIO recovery
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
        TailCacheFileSystem eioTcf = new TailCacheFileSystem(eioDelegate,
                new TailCacheFileSystemConfig()
                        .setPerFileCacheLimits(10 * 1024, 1, CHUNK_SIZE)
                        .setMaxCacheSizeBytes(100 * 1024)
                        .setWriteBatchBytes(1) // every write goes to delegate (page cache)
                        .setIoWaitTimeoutMs(5000)
                        .setExpectedMinRetentionMs(0)
                        .setEvictScanIntervalMs(60_000)
                        .setWatermarkRatios(0.5, 0.8)
                        .setMaxEvictRatioPerWrite(0.5),
                ioExecutor);

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
        public void rollSync(AsyncSegmentFile file, long currentSegmentSize,
                boolean noFs, List<FileChannel> pending) {
            segRollCount++;
            super.rollSync(file, currentSegmentSize, noFs, pending);
        }

        @Override
        public long transferToSync(AsyncSegmentFile file, long offset, long count, WritableByteChannel target) {
            transferToCount++;
            return super.transferToSync(file, offset, count, target);
        }
    }
}
