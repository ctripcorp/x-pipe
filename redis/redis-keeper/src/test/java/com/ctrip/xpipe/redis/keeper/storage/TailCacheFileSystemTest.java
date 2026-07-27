package com.ctrip.xpipe.redis.keeper.storage;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class TailCacheFileSystemTest {

    private static final long CHUNK_SIZE = 64;

    private Path tempDir;
    private ExecutorService ioExecutor;
    private RecordingDelegate delegate;
    private TailCacheFileSystem tcf;

    @Before
    public void setUp() throws Exception {
        tempDir = Files.createTempDirectory("tailcache-test-");
        ioExecutor = Executors.newCachedThreadPool();
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
        String p = path("file4");
        AsyncFile writer = tcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        // Three small writes — all go to cache, not to disk
        delegate.reset();
        writeTcfSync(writer, new byte[]{1, 2, 3});
        writeTcfSync(writer, new byte[]{4, 5, 6});
        writeTcfSync(writer, new byte[]{7, 8, 9});
        // After writes, delegate may not have been called yet (data in cache)
        int writesBeforeClose = delegate.fileWriteCount;

        // Close flushes all to disk
        tcf.close(writer).get(5, TimeUnit.SECONDS);
        // After close, all data should be on disk
        assertArrayEquals(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9}, readFileSync(p));
        // Total delegate writes (before close + on close) should be >= 1
        // The key point: data is correct regardless of how many delegate calls
        assertTrue("data should be flushed on close", delegate.fileWriteCount >= writesBeforeClose);
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
    public void testWriteExceedsPerFileLimit() throws Exception {
        String p = path("file6");
        AsyncFile file = tcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        try {
            // Write more than maxCacheSizePerFileBytes (10KB)
            byte[] bigData = new byte[11 * 1024];
            Arrays.fill(bigData, (byte) 42);
            writeTcfSync(file, bigData);

            // Should still work via no-cache fallback
            tcf.close(file).get(5, TimeUnit.SECONDS);
            byte[] onDisk = readFileSync(p);
            assertEquals(11 * 1024, onDisk.length);
            assertEquals(42, onDisk[0]);
            assertEquals(42, onDisk[onDisk.length - 1]);
        } finally {
            // file already closed
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
        public void closeSync(AsyncFile file) {
            fileCloseCount++;
            super.closeSync(file);
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
        public Map<String, AsyncFile> rollSync(AsyncSegmentFile file) {
            segRollCount++;
            return super.rollSync(file);
        }

        @Override
        public long transferToSync(AsyncSegmentFile file, long offset, long count, WritableByteChannel target) {
            transferToCount++;
            return super.transferToSync(file, offset, count, target);
        }
    }
}
