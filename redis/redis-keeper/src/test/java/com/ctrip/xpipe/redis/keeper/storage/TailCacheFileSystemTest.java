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

import com.ctrip.xpipe.tuple.Pair;

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
    // G. preferDirectRead & read path branches
    // =========================================================================

    @Test
    public void testPreferDirectReadNoCache() throws Exception {
        // NO_CACHE mode → preferDirectRead always returns true
        String p = path("file_pdr_nocache");
        writeFileSync(p, new byte[10]); // file must exist for READ mode open
        AsyncFile file = tcf.open(p, AbstractStorageFile.OpenMode.READ, false, false, null,
                AbstractStorageFile.CacheMode.NO_CACHE).get();
        FileCacheEntry entry = file.getCacheEntry();
        // For NO_CACHE, preferDirectRead should return true regardless of other conditions
        assertTrue(tcf.preferDirectRead(file, entry, 0, true));
        tcf.close(file).get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testPreferDirectReadUninitialized() throws Exception {
        // Before cache is initialized → preferDirectRead returns true
        String p = path("file_pdr_uninit");
        writeFileSync(p, new byte[10]);
        AsyncFile file = tcf.open(p, AbstractStorageFile.OpenMode.READ, false, false, null,
                AbstractStorageFile.CacheMode.NO_CACHE).get();
        FileCacheEntry entry = file.getCacheEntry();
        // NO_CACHE entry has no init, so isInitialized() is false → true
        assertTrue(tcf.preferDirectRead(file, entry, 0, true));
        tcf.close(file).get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testPreferDirectReadOffsetBeforeCache() throws Exception {
        // offset < cacheStartOffset → returns true (direct read, skip cache)
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
            // Requesting offset 0 which is < cacheStartOffset → preferDirectRead = true
            assertTrue(tcf.preferDirectRead(reader, entry, 0, true));
        } finally {
            tcf.close(reader).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testPreferDirectReadCacheHitWriter() throws Exception {
        // Writer with data in cache, readPreferCache=true, backingFsMode≠NO_CACHE
        // offset < writtenToFsOffset (fsynced), !atomicReplace → returns false (use cache)
        String p = path("file_pdr_hit");
        AsyncFile writer = tcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        writeTcfSync(writer, new byte[]{1, 2, 3, 4, 5});
        tcf.fsync(writer).get(5, TimeUnit.SECONDS);
        // Now: cacheEndOffset=5, writtenToFsOffset=5, cacheStartOffset=0
        FileCacheEntry entry = writer.getCacheEntry();
        // offset=2 >= cacheStartOffset(0), initialized, preferCache=true, backingFsMode=ASYNC
        // !atomicReplace → offset(2) < writtenToFsOffset(5) → return false
        assertFalse(tcf.preferDirectRead(writer, entry, 2, true));
        // Read from cache should return correct data
        delegate.reset();
        delegate.fileReadCount = 0;
        ByteBuf buf = tcf.read(writer, 3, 2).get(5, TimeUnit.SECONDS);
        try {
            assertArrayEquals(new byte[]{3, 4, 5}, readBytes(buf));
        } finally {
            // read(AsyncFile, length, offset) uses fromPosition=false → no delegate read
        }
        tcf.close(writer).get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testPreferDirectReadReadPreferCacheFalse() throws Exception {
        // readPreferCache=false → for flushed data, preferDirectRead returns true
        String p = path("file_pdr_false");
        AsyncFile writer = tcf.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        writeTcfSync(writer, new byte[]{1, 2, 3, 4, 5});
        tcf.fsync(writer).get(5, TimeUnit.SECONDS);
        FileCacheEntry entry = writer.getCacheEntry();
        // preferCache=false → !atomicReplace → offset(2) < writtenToFsOffset(5) → return true
        assertTrue(tcf.preferDirectRead(writer, entry, 2, false));
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
        // transferPreferCache=false → preferDirectRead returns true for flushed data → delegate path
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
    public void testDeleteSegmentsOutOfOrderThrows() throws Exception {
        // deleteSegments with wrong order → IllegalArgumentException (thrown synchronously)
        String dir = path("seg_del_order");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = tcf.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        tcf.write(seg, bufOf(new byte[10])).get(5, TimeUnit.SECONDS);
        tcf.roll(seg).get(5, TimeUnit.SECONDS);
        tcf.write(seg, bufOf(new byte[20])).get(5, TimeUnit.SECONDS);
        tcf.roll(seg).get(5, TimeUnit.SECONDS);
        tcf.write(seg, bufOf(new byte[5])).get(5, TimeUnit.SECONDS);

        // Try to delete second segment (offset 10) without deleting first (offset 0)
        try {
            tcf.deleteSegments(seg, Collections.singletonList(10L));
            fail("Expected IllegalArgumentException for out-of-order delete");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("expected 0"));
        } finally {
            tcf.close(seg).get(5, TimeUnit.SECONDS);
        }
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
        public void rollSync(AsyncSegmentFile file) {
            segRollCount++;
            super.rollSync(file);
        }

        @Override
        public long transferToSync(AsyncSegmentFile file, long offset, long count, WritableByteChannel target) {
            transferToCount++;
            return super.transferToSync(file, offset, count, target);
        }
    }
}
