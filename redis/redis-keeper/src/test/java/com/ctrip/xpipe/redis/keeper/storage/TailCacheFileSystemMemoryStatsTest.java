package com.ctrip.xpipe.redis.keeper.storage;

import io.netty.buffer.Unpooled;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class TailCacheFileSystemMemoryStatsTest {

    private static final long CHUNK_SIZE = 64;
    private static final long MAX_CACHE_BYTES = 100 * 1024;
    private static final String SEG_PREFIX = "seg";
    private static final String IDX_PREFIX = "idx";
    private static final List<String> INDEX_PREFIXES = Collections.singletonList(IDX_PREFIX);

    private Path tempDir;
    private ExecutorService ioExecutor;
    private TailCacheFileSystem fs;

    @Before
    public void setUp() throws Exception {
        tempDir = Files.createTempDirectory("tailcache-mem-stats-");
        ioExecutor = Executors.newCachedThreadPool();
        // no fsync until explicit fsync
        AsyncTFSBasedFileSystem backing = new AsyncTFSBasedFileSystem(ioExecutor, 1024 * 1024,
                TimeUnit.HOURS.toMillis(1));
        TailCacheFileSystemConfig config = new TailCacheFileSystemConfig();
        config.setPerFileCacheLimits(10 * 1024, 1, CHUNK_SIZE);
        config.setMaxCacheSizeBytes(MAX_CACHE_BYTES);
        config.setWriteBatchBytes(128);
        config.setIoWaitTimeoutMs(5000);
        config.setExpectedMinRetentionMs(0);
        config.setEvictScanIntervalMs(60_000);
        fs = new TailCacheFileSystem(backing, config, ioExecutor);
    }

    @After
    public void tearDown() {
        fs.shutdown();
        deleteRecursively(tempDir.toFile());
    }

    @Test
    public void testEmpty() {
        TailCacheMemoryStats stats = fs.getMemoryStats();
        assertEquals(MAX_CACHE_BYTES, stats.getMaxCacheSizeBytes());
        assertEquals(0, stats.getCommittedBytes());
        assertEquals(0, stats.getUnDurableBytes());
        assertTrue(fs.listFileMemoryStats().isEmpty());
    }

    @Test
    public void testFileCachedOnlyIsUnDurableUntilFsync() throws Exception {
        String path = tempDir.resolve("meta").toString();
        AsyncFile file = openWriter(path);
        // below writeBatchBytes: stays in cache only
        fs.write(file, Unpooled.wrappedBuffer(new byte[30])).get(5, TimeUnit.SECONDS);

        TailCacheFileMemoryStats row = single(fs.listFileMemoryStats());
        assertEquals(TailCacheFileMemoryStats.Kind.FILE, row.getKind());
        assertEquals(path, row.getPath());
        assertEquals(30, row.getUnDurableBytes());
        assertEquals(file.getCacheEntry().ownCacheSizeBytes(), row.getCacheSizeBytes());
        assertTrue(row.getCacheSizeBytes() > 0);

        TailCacheMemoryStats stats = fs.getMemoryStats();
        assertEquals(30, stats.getUnDurableBytes());
        assertEquals(fs.getGlobalCommittedBytes(), stats.getCommittedBytes());
        assertEquals(row.getCacheSizeBytes(), stats.getCommittedBytes());

        fs.fsync(file).get(5, TimeUnit.SECONDS);
        assertEquals(0, single(fs.listFileMemoryStats()).getUnDurableBytes());
        assertEquals(0, fs.getMemoryStats().getUnDurableBytes());
        fs.close(file).get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testWrittenButNotFsyncedIsUnDurable() throws Exception {
        AsyncFile file = openWriter(tempDir.resolve("rdb").toString());
        // reaches writeBatchBytes: written to the backing FS, but not fsynced
        fs.write(file, Unpooled.wrappedBuffer(new byte[200])).get(5, TimeUnit.SECONDS);
        FileCacheEntry entry = file.getCacheEntry();
        long deadline = System.currentTimeMillis() + 5000;
        while (entry.writtenToFsOffset < 200 && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertEquals(200, entry.writtenToFsOffset);
        assertEquals(200, single(fs.listFileMemoryStats()).getUnDurableBytes());

        fs.fsync(file).get(5, TimeUnit.SECONDS);
        assertEquals(0, fs.getMemoryStats().getUnDurableBytes());
        fs.close(file).get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testSegmentAndIndexAreSeparateRows() throws Exception {
        String dir = tempDir.resolve("cmd").toString();
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile writer = fs.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get(5, TimeUnit.SECONDS);
        fs.write(writer, Unpooled.wrappedBuffer(new byte[30])).get(5, TimeUnit.SECONDS);
        AsyncFile idx = fs.getCurrentIndexFiles(writer, INDEX_PREFIXES).get(5, TimeUnit.SECONDS)
                .getValue().get(IDX_PREFIX);
        fs.write(idx, Unpooled.wrappedBuffer(new byte[20])).get(5, TimeUnit.SECONDS);

        List<TailCacheFileMemoryStats> rows = fs.listFileMemoryStats();
        assertEquals(2, rows.size());
        TailCacheFileMemoryStats segment = ofKind(rows, TailCacheFileMemoryStats.Kind.SEGMENT);
        TailCacheFileMemoryStats index = ofKind(rows, TailCacheFileMemoryStats.Kind.INDEX);
        assertEquals(writer.path, segment.getPath());
        assertEquals(Paths.get(dir, IDX_PREFIX + writer.openedSegmentStartOffset).toString(), index.getPath());
        assertEquals(idx.path, index.getPath());
        assertEquals(30, segment.getUnDurableBytes());
        assertEquals(20, index.getUnDurableBytes());

        SegmentFileCacheEntry segEntry = (SegmentFileCacheEntry) writer.getCacheEntry();
        assertEquals(segEntry.ownCacheSizeBytes(), segment.getCacheSizeBytes());
        assertEquals(idx.getCacheEntry().ownCacheSizeBytes(), index.getCacheSizeBytes());
        assertEquals("segment row excludes index", segEntry.cacheSizeBytes(),
                segment.getCacheSizeBytes() + index.getCacheSizeBytes());
        assertEquals(50, fs.getMemoryStats().getUnDurableBytes());

        fs.close(writer).get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testClosedEntryIsGone() throws Exception {
        AsyncFile file = openWriter(tempDir.resolve("closed").toString());
        fs.write(file, Unpooled.wrappedBuffer(new byte[30])).get(5, TimeUnit.SECONDS);
        assertEquals(1, fs.listFileMemoryStats().size());

        fs.close(file).get(5, TimeUnit.SECONDS);
        assertTrue(fs.listFileMemoryStats().isEmpty());
        TailCacheMemoryStats stats = fs.getMemoryStats();
        assertEquals(0, stats.getCommittedBytes());
        assertEquals(0, stats.getUnDurableBytes());
    }

    @Test
    public void testKeyPathDecoding() {
        String file = Paths.get("/data", "keeper", "meta").toString();
        assertEquals(file, StorageUtil.pathOfFileKey(StorageUtil.asyncFileKey(file)));
        String dir = Paths.get("/data", "keeper", "cmd").toString();
        String segmentKey = StorageUtil.segmentKey(dir, "cmd_");
        assertEquals(Paths.get(dir, "cmd_").toString(), StorageUtil.pathOfSegmentKey(segmentKey));
        assertEquals(Paths.get(dir, "idx_100").toString(),
                StorageUtil.indexPathOfSegmentKey(segmentKey, "idx_", 100));
    }

    private AsyncFile openWriter(String path) throws Exception {
        return fs.open(path, AbstractStorageFile.OpenMode.WRITE, AbstractStorageFile.ReplaceMode.NORMAL,
                false, null).get(5, TimeUnit.SECONDS);
    }

    private static TailCacheFileMemoryStats single(List<TailCacheFileMemoryStats> rows) {
        assertEquals(rows.toString(), 1, rows.size());
        return rows.get(0);
    }

    private static TailCacheFileMemoryStats ofKind(List<TailCacheFileMemoryStats> rows,
                                                   TailCacheFileMemoryStats.Kind kind) {
        return rows.stream().filter(r -> r.getKind() == kind).findFirst()
                .orElseThrow(() -> new AssertionError("no " + kind + " in " + rows));
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
}
