package com.ctrip.xpipe.redis.keeper.storage;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * Benchmark comparing three layers of single-file read/write overhead:
 *
 * <ul>
 *   <li>TailCacheFileSystem (cache-miss path) — wrapper executor waits for IO task completion</li>
 *   <li>AsyncTFSBasedFileSystem — sync path</li>
 *   <li>Direct FileChannel — baseline</li>
 * </ul>
 *
 * Both AsyncFile and AsyncSegmentFile variants are tested. SegmentFile uses a single segment (no roll).
 * All parameters use defaults: chunkSize=1MB, writeBatchBytes=1MB, write size=1MB.
 *
 * Run via main() for a consolidated results table.
 */
public class StorageBenchmark {

    // TrackingExecutor is in its own file (shared with TailCacheFileSystemTest)

    // =========================================================================
    // Benchmark parameters (all defaults)
    // =========================================================================

    private static final int CHUNK_SIZE = 1024 * 1024;       // 1MB
    private static final int TOTAL_ITERATIONS = 64;          // 64MB total
    private static final int WARMUP_ITERATIONS = 16;         // 16MB warmup

    private static final String SEG_PREFIX = "seg";
    private static final List<String> INDEX_PREFIXES = Collections.singletonList("idx");

    // =========================================================================
    // Setup / Teardown
    // =========================================================================

    private Path tempDir;
    private TrackingExecutor trackingIo;
    private ExecutorService ioExecutor;
    private TailCacheFileSystem tailFs;
    private AsyncTFSBasedFileSystem tfs;

    private void setUp() throws Exception {
        tempDir = Files.createTempDirectory("storage-bench-");
        trackingIo = new TrackingExecutor(Executors.newFixedThreadPool(10));
        ioExecutor = Executors.newFixedThreadPool(10);

        TailCacheFileSystemConfig config = new TailCacheFileSystemConfig();
        config.setPerFileCacheLimits(100 * 1024 * 1024, 1, 1024 * 1024);  // maxCacheSizePerFile=100MB, chunkSize=1MB
        config.setMaxCacheSizeBytes(1024 * 1024 * 1024);                    // 1GB
        config.setWriteBatchBytes(1024 * 1024);                             // 1MB
        config.setIoWaitTimeoutMs(10000);
        config.setExpectedMinRetentionMs(0);
        config.setEvictScanIntervalMs(Long.MAX_VALUE / 2);

        AsyncTFSBasedFileSystem backingFs = new AsyncTFSBasedFileSystem(
                Executors.newCachedThreadPool(), Long.MAX_VALUE / 2, Long.MAX_VALUE / 2_000_000L);
        tailFs = new TailCacheFileSystem(backingFs, config, trackingIo);

        tfs = new AsyncTFSBasedFileSystem(
                Executors.newCachedThreadPool(), Long.MAX_VALUE / 2, Long.MAX_VALUE / 2_000_000L);
    }

    private void tearDown() throws Exception {
        tailFs.shutdown();
        tfs.shutdown();
        trackingIo.shutdown();
        ioExecutor.shutdown();
        deleteRecursively(tempDir.toFile());
    }

    private String path(String name) {
        return tempDir.resolve(name).toString();
    }

    private ByteBuf bufOf(byte[] data) {
        return Unpooled.wrappedBuffer(data);
    }

    // openSync only builds the file object; openWithFileEntry registers the FileEntry and opens
    // the channels (for segments it also runs initCurrentChannelsSync).
    private static final BiConsumer<String, CompletableFuture<?>> NO_REGISTER = (key, future) -> { };
    private static final BiConsumer<String, List<FileChannel>> CLOSE_CHANNELS =
            (path, channels) -> StorageUtil.closeChannels(channels);
    private static final long RECOVER_TIMEOUT_MS = 20_000;
    private static final long IO_TIMEOUT_MS = 10_000;

    private AsyncFile openTfsFile(String filePath, AbstractStorageFile.OpenMode openMode) {
        String key = StorageUtil.asyncFileKey(filePath);
        AsyncFile file = tfs.openSync(filePath, key, key, openMode, false, false, null, false);
        return tfs.openWithFileEntry(file, false, NO_REGISTER, CLOSE_CHANNELS,
                RECOVER_TIMEOUT_MS, IO_TIMEOUT_MS);
    }

    private AsyncSegmentFile openTfsSeg(String dirPath, boolean write) {
        String key = StorageUtil.segmentKey(dirPath, SEG_PREFIX);
        AsyncSegmentFile seg = tfs.openSync(dirPath, SEG_PREFIX, key, key, INDEX_PREFIXES, write, null, false);
        return tfs.openWithFileEntry(seg, false, NO_REGISTER, CLOSE_CHANNELS,
                RECOVER_TIMEOUT_MS, IO_TIMEOUT_MS);
    }

    private void rollSeg(AsyncSegmentFile seg) throws Exception {
        long size = seg.currentSegmentChannel == null ? 0L : seg.currentSegmentChannel.size();
        StorageUtil.closeChannels(tfs.rollMetadataSync(seg, size, false));
        tfs.initCurrentChannelsSync(seg);
    }

    private void writeSeg(AsyncSegmentFile seg, ByteBuf data) throws Exception {
        tfs.writeSync(seg, data);
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

    // =========================================================================
    // AsyncFile — Write benchmarks (return MB/s)
    // =========================================================================

    private double doWrite(int iterations, Runnable write, Runnable afterBatch, Runnable fsync) throws Exception {
        for (int i = 0; i < WARMUP_ITERATIONS; i++) write.run();
        afterBatch.run();
        fsync.run();

        long start = System.nanoTime();
        for (int i = 0; i < iterations; i++) write.run();
        afterBatch.run();
        long elapsed = System.nanoTime() - start;
        return (double) iterations * CHUNK_SIZE / (elapsed / 1_000_000_000.0) / (1024 * 1024);
    }

    private double benchAsyncFileWrite_TailCache() throws Exception {
        String p = path("bench_af_write_tail");
        AsyncFile file = tailFs.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        byte[] data = new byte[CHUNK_SIZE];

        double mbps = doWrite(TOTAL_ITERATIONS,
                () -> tailFs.write(file, bufOf(data).retain()),
                trackingIo::awaitAndClear,
                () -> { try { tailFs.fsync(file).get(); } catch (Exception e) { throw new RuntimeException(e); } });

        tailFs.close(file).get();
        return mbps;
    }

    private double benchAsyncFileWrite_TFS() throws Exception {
        String p = path("bench_af_write_tfs");
        AsyncFile file = openTfsFile(p, AbstractStorageFile.OpenMode.WRITE);
        byte[] data = new byte[CHUNK_SIZE];

        double mbps = doWrite(TOTAL_ITERATIONS,
                () -> tfs.writeSync(file, bufOf(data)),
                () -> {},
                () -> tfs.fsyncSync(file));

        StorageUtil.closeChannels(tfs.closeSync(file));
        return mbps;
    }

    private double benchAsyncFileWrite_Direct() throws Exception {
        String p = path("bench_af_write_direct");
        try (FileChannel ch = FileChannel.open(Paths.get(p),
                StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            ByteBuffer buf = ByteBuffer.allocateDirect(CHUNK_SIZE);

            double mbps = doWrite(TOTAL_ITERATIONS,
                    () -> { buf.clear(); try { while (buf.hasRemaining()) ch.write(buf); } catch (Exception e) { throw new RuntimeException(e); } },
                    () -> {},
                    () -> { try { ch.force(true); } catch (Exception e) { throw new RuntimeException(e); } });

            return mbps;
        }
    }

    private double benchAsyncFileWrite_TFSOnIoThread() throws Exception {
        String p = path("bench_af_write_tfs_io");
        AsyncFile file = openTfsFile(p, AbstractStorageFile.OpenMode.WRITE);
        byte[] data = new byte[CHUNK_SIZE];

        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            CompletableFuture.supplyAsync(() -> tfs.writeSync(file, bufOf(data)), ioExecutor).get();
        }
        CompletableFuture.runAsync(() -> tfs.fsyncSync(file), ioExecutor).get();

        long start = System.nanoTime();
        for (int i = 0; i < TOTAL_ITERATIONS; i++) {
            CompletableFuture.supplyAsync(() -> tfs.writeSync(file, bufOf(data)), ioExecutor).get();
        }
        long elapsed = System.nanoTime() - start;

        StorageUtil.closeChannels(tfs.closeSync(file));
        return (double) TOTAL_ITERATIONS * CHUNK_SIZE / (elapsed / 1_000_000_000.0) / (1024 * 1024);
    }

    private double benchAsyncFileWrite_DirectOnIoThread() throws Exception {
        String p = path("bench_af_write_direct_io");
        try (FileChannel ch = FileChannel.open(Paths.get(p),
                StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            ByteBuffer buf = ByteBuffer.allocateDirect(CHUNK_SIZE);

            for (int i = 0; i < WARMUP_ITERATIONS; i++) {
                CompletableFuture.supplyAsync(() -> {
                    buf.clear();
                    try { while (buf.hasRemaining()) ch.write(buf); }
                    catch (Exception e) { throw new RuntimeException(e); }
                    return null;
                }, ioExecutor).get();
            }
            CompletableFuture.supplyAsync(() -> {
                try { ch.force(true); } catch (Exception e) { throw new RuntimeException(e); }
                return null;
            }, ioExecutor).get();

            long start = System.nanoTime();
            for (int i = 0; i < TOTAL_ITERATIONS; i++) {
                CompletableFuture.supplyAsync(() -> {
                    buf.clear();
                    try { while (buf.hasRemaining()) ch.write(buf); }
                    catch (Exception e) { throw new RuntimeException(e); }
                    return null;
                }, ioExecutor).get();
            }
            long elapsed = System.nanoTime() - start;

            return (double) TOTAL_ITERATIONS * CHUNK_SIZE / (elapsed / 1_000_000_000.0) / (1024 * 1024);
        }
    }

    // =========================================================================
    // AsyncFile — Read benchmarks (return MB/s)
    // =========================================================================

    private double benchAsyncFileRead_TailCache() throws Exception {
        String p = path("bench_af_read_tail");
        AsyncFile writer = tailFs.open(p, AbstractStorageFile.OpenMode.WRITE, false, false, null).get();
        byte[] data = new byte[CHUNK_SIZE];
        for (int i = 0; i < TOTAL_ITERATIONS; i++) {
            tailFs.write(writer, bufOf(data).retain());
        }
        tailFs.fsync(writer).get();
        tailFs.close(writer).get();
        trackingIo.clear();

        AsyncFile reader = tailFs.open(p, AbstractStorageFile.OpenMode.READ, false, false, null).get();
        // Clear cache so reads go through TailCacheFileSystem but miss cache (fall through to TFS).
        // After reset: cacheStartOffset=-1, isInitialized()=false, preferCacheRead()=(false, true),
        // all reads submit to ioExecutor -> delegate.readSync (TFS path).
        FileCacheEntry entry = reader.getCacheEntry();
        synchronized (entry) {
            entry.reset();
        }

        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            ByteBuf buf = tailFs.read(reader, CHUNK_SIZE, (long) i * CHUNK_SIZE).get();
            buf.release();
        }

        long start = System.nanoTime();
        for (int i = 0; i < TOTAL_ITERATIONS; i++) {
            ByteBuf buf = tailFs.read(reader, CHUNK_SIZE, (long) i * CHUNK_SIZE).get();
            buf.release();
        }
        long elapsed = System.nanoTime() - start;

        tailFs.close(reader).get();
        return (double) TOTAL_ITERATIONS * CHUNK_SIZE / (elapsed / 1_000_000_000.0) / (1024 * 1024);
    }

    private double benchAsyncFileRead_TFS() throws Exception {
        String p = path("bench_af_read_tfs");
        try (FileChannel ch = FileChannel.open(Paths.get(p),
                StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            ByteBuffer buf = ByteBuffer.allocateDirect(CHUNK_SIZE);
            for (int i = 0; i < TOTAL_ITERATIONS; i++) {
                buf.clear();
                while (buf.hasRemaining()) ch.write(buf);
            }
            ch.force(true);
        }

        AsyncFile reader = openTfsFile(p, AbstractStorageFile.OpenMode.READ);

        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            ByteBuf buf = tfs.readSync(reader, CHUNK_SIZE, (long) i * CHUNK_SIZE, 0);
            buf.release();
        }

        long start = System.nanoTime();
        for (int i = 0; i < TOTAL_ITERATIONS; i++) {
            ByteBuf buf = tfs.readSync(reader, CHUNK_SIZE, (long) i * CHUNK_SIZE, 0);
            buf.release();
        }
        long elapsed = System.nanoTime() - start;

        StorageUtil.closeChannels(tfs.closeSync(reader));
        return (double) TOTAL_ITERATIONS * CHUNK_SIZE / (elapsed / 1_000_000_000.0) / (1024 * 1024);
    }

    private double benchAsyncFileRead_Direct() throws Exception {
        String p = path("bench_af_read_direct");
        try (FileChannel ch = FileChannel.open(Paths.get(p),
                StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            ByteBuffer buf = ByteBuffer.allocateDirect(CHUNK_SIZE);
            for (int i = 0; i < TOTAL_ITERATIONS; i++) {
                buf.clear();
                while (buf.hasRemaining()) ch.write(buf);
            }
            ch.force(true);
        }

        try (FileChannel ch = FileChannel.open(Paths.get(p), StandardOpenOption.READ)) {
            ByteBuffer buf = ByteBuffer.allocateDirect(CHUNK_SIZE);

            for (int i = 0; i < WARMUP_ITERATIONS; i++) {
                buf.clear();
                long pos = (long) i * CHUNK_SIZE;
                while (buf.hasRemaining()) {
                    int n = ch.read(buf, pos + buf.position());
                    if (n < 0) break;
                }
            }

            long start = System.nanoTime();
            for (int i = 0; i < TOTAL_ITERATIONS; i++) {
                buf.clear();
                long pos = (long) i * CHUNK_SIZE;
                while (buf.hasRemaining()) {
                    int n = ch.read(buf, pos + buf.position());
                    if (n < 0) break;
                }
            }
            long elapsed = System.nanoTime() - start;

            return (double) TOTAL_ITERATIONS * CHUNK_SIZE / (elapsed / 1_000_000_000.0) / (1024 * 1024);
        }
    }

    private double benchAsyncFileRead_TFSOnIoThread() throws Exception {
        String p = path("bench_af_read_tfs_io");
        try (FileChannel ch = FileChannel.open(Paths.get(p),
                StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            ByteBuffer buf = ByteBuffer.allocateDirect(CHUNK_SIZE);
            for (int i = 0; i < TOTAL_ITERATIONS; i++) {
                buf.clear();
                while (buf.hasRemaining()) ch.write(buf);
            }
            ch.force(true);
        }

        AsyncFile reader = openTfsFile(p, AbstractStorageFile.OpenMode.READ);

        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            long offset = (long) i * CHUNK_SIZE;
            ByteBuf buf = CompletableFuture.supplyAsync(() -> tfs.readSync(reader, CHUNK_SIZE, offset, 0), ioExecutor).get();
            buf.release();
        }

        long start = System.nanoTime();
        for (int i = 0; i < TOTAL_ITERATIONS; i++) {
            long offset = (long) i * CHUNK_SIZE;
            ByteBuf buf = CompletableFuture.supplyAsync(() -> tfs.readSync(reader, CHUNK_SIZE, offset, 0), ioExecutor).get();
            buf.release();
        }
        long elapsed = System.nanoTime() - start;

        StorageUtil.closeChannels(tfs.closeSync(reader));
        return (double) TOTAL_ITERATIONS * CHUNK_SIZE / (elapsed / 1_000_000_000.0) / (1024 * 1024);
    }

    private double benchAsyncFileRead_DirectOnIoThread() throws Exception {
        String p = path("bench_af_read_direct_io");
        try (FileChannel ch = FileChannel.open(Paths.get(p),
                StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            ByteBuffer buf = ByteBuffer.allocateDirect(CHUNK_SIZE);
            for (int i = 0; i < TOTAL_ITERATIONS; i++) {
                buf.clear();
                while (buf.hasRemaining()) ch.write(buf);
            }
            ch.force(true);
        }

        try (FileChannel ch = FileChannel.open(Paths.get(p), StandardOpenOption.READ)) {
            ByteBuffer buf = ByteBuffer.allocateDirect(CHUNK_SIZE);

            for (int i = 0; i < WARMUP_ITERATIONS; i++) {
                long pos = (long) i * CHUNK_SIZE;
                CompletableFuture.supplyAsync(() -> {
                    buf.clear();
                    try { while (buf.hasRemaining()) { int n = ch.read(buf, pos + buf.position()); if (n < 0) break; } }
                    catch (Exception e) { throw new RuntimeException(e); }
                    return null;
                }, ioExecutor).get();
            }

            long start = System.nanoTime();
            for (int i = 0; i < TOTAL_ITERATIONS; i++) {
                long pos = (long) i * CHUNK_SIZE;
                CompletableFuture.supplyAsync(() -> {
                    buf.clear();
                    try { while (buf.hasRemaining()) { int n = ch.read(buf, pos + buf.position()); if (n < 0) break; } }
                    catch (Exception e) { throw new RuntimeException(e); }
                    return null;
                }, ioExecutor).get();
            }
            long elapsed = System.nanoTime() - start;

            return (double) TOTAL_ITERATIONS * CHUNK_SIZE / (elapsed / 1_000_000_000.0) / (1024 * 1024);
        }
    }

    // =========================================================================
    // AsyncSegmentFile — Write benchmarks (return MB/s)
    // =========================================================================

    private double benchAsyncSegmentFileWrite_TailCache() throws Exception {
        String dir = path("bench_sf_write_tail");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile file = tailFs.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        byte[] data = new byte[CHUNK_SIZE];

        double mbps = doWrite(TOTAL_ITERATIONS,
                () -> tailFs.write(file, bufOf(data).retain()),
                trackingIo::awaitAndClear,
                () -> { try { tailFs.fsync(file).get(); } catch (Exception e) { throw new RuntimeException(e); } });

        tailFs.close(file).get();
        return mbps;
    }

    private double benchAsyncSegmentFileWrite_TFS() throws Exception {
        String dir = path("bench_sf_write_tfs");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile file = openTfsSeg(dir, true);
        byte[] data = new byte[CHUNK_SIZE];

        double mbps = doWrite(TOTAL_ITERATIONS,
                () -> tfs.writeSync(file, bufOf(data)),
                () -> {},
                () -> tfs.fsyncSync(file));

        StorageUtil.closeChannels(tfs.closeSync(file));
        return mbps;
    }

    private double benchAsyncSegmentFileWrite_Direct() throws Exception {
        String p = path("bench_sf_write_direct");
        try (FileChannel ch = FileChannel.open(Paths.get(p),
                StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            ByteBuffer buf = ByteBuffer.allocateDirect(CHUNK_SIZE);

            double mbps = doWrite(TOTAL_ITERATIONS,
                    () -> { buf.clear(); try { while (buf.hasRemaining()) ch.write(buf); } catch (Exception e) { throw new RuntimeException(e); } },
                    () -> {},
                    () -> { try { ch.force(true); } catch (Exception e) { throw new RuntimeException(e); } });

            return mbps;
        }
    }

    private double benchAsyncSegmentFileWrite_TFSOnIoThread() throws Exception {
        String dir = path("bench_sf_write_tfs_io");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile file = openTfsSeg(dir, true);
        byte[] data = new byte[CHUNK_SIZE];

        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            CompletableFuture.supplyAsync(() -> tfs.writeSync(file, bufOf(data)), ioExecutor).get();
        }
        CompletableFuture.runAsync(() -> tfs.fsyncSync(file), ioExecutor).get();

        long start = System.nanoTime();
        for (int i = 0; i < TOTAL_ITERATIONS; i++) {
            CompletableFuture.supplyAsync(() -> tfs.writeSync(file, bufOf(data)), ioExecutor).get();
        }
        long elapsed = System.nanoTime() - start;

        StorageUtil.closeChannels(tfs.closeSync(file));
        return (double) TOTAL_ITERATIONS * CHUNK_SIZE / (elapsed / 1_000_000_000.0) / (1024 * 1024);
    }

    private double benchAsyncSegmentFileWrite_DirectOnIoThread() throws Exception {
        String p = path("bench_sf_write_direct_io");
        try (FileChannel ch = FileChannel.open(Paths.get(p),
                StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            ByteBuffer buf = ByteBuffer.allocateDirect(CHUNK_SIZE);

            for (int i = 0; i < WARMUP_ITERATIONS; i++) {
                CompletableFuture.supplyAsync(() -> {
                    buf.clear();
                    try { while (buf.hasRemaining()) ch.write(buf); }
                    catch (Exception e) { throw new RuntimeException(e); }
                    return null;
                }, ioExecutor).get();
            }
            CompletableFuture.supplyAsync(() -> {
                try { ch.force(true); } catch (Exception e) { throw new RuntimeException(e); }
                return null;
            }, ioExecutor).get();

            long start = System.nanoTime();
            for (int i = 0; i < TOTAL_ITERATIONS; i++) {
                CompletableFuture.supplyAsync(() -> {
                    buf.clear();
                    try { while (buf.hasRemaining()) ch.write(buf); }
                    catch (Exception e) { throw new RuntimeException(e); }
                    return null;
                }, ioExecutor).get();
            }
            long elapsed = System.nanoTime() - start;

            return (double) TOTAL_ITERATIONS * CHUNK_SIZE / (elapsed / 1_000_000_000.0) / (1024 * 1024);
        }
    }

    // =========================================================================
    // AsyncSegmentFile — Read benchmarks (return MB/s)
    // =========================================================================

    private double benchAsyncSegmentFileRead_TailCache() throws Exception {
        String dir = path("bench_sf_read_tail");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile writer = tailFs.open(dir, SEG_PREFIX, INDEX_PREFIXES, true, null).get();
        byte[] data = new byte[CHUNK_SIZE];
        for (int i = 0; i < TOTAL_ITERATIONS; i++) {
            tailFs.write(writer, bufOf(data).retain());
        }
        tailFs.fsync(writer).get();
        tailFs.close(writer).get();
        trackingIo.clear();

        AsyncSegmentFile reader = tailFs.open(dir, SEG_PREFIX, INDEX_PREFIXES, false, null).get();
        FileCacheEntry entry = reader.getCacheEntry();
        synchronized (entry) {
            entry.reset();
        }

        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            ByteBuf buf = tailFs.read(reader, CHUNK_SIZE, (long) i * CHUNK_SIZE).get();
            buf.release();
        }

        long start = System.nanoTime();
        for (int i = 0; i < TOTAL_ITERATIONS; i++) {
            ByteBuf buf = tailFs.read(reader, CHUNK_SIZE, (long) i * CHUNK_SIZE).get();
            buf.release();
        }
        long elapsed = System.nanoTime() - start;

        tailFs.close(reader).get();
        return (double) TOTAL_ITERATIONS * CHUNK_SIZE / (elapsed / 1_000_000_000.0) / (1024 * 1024);
    }

    private double benchAsyncSegmentFileRead_TFS() throws Exception {
        String dir = path("bench_sf_read_tfs");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile writer = openTfsSeg(dir, true);
        for (int i = 0; i < TOTAL_ITERATIONS; i++) {
            writeSeg(writer, bufOf(new byte[CHUNK_SIZE]));
        }
        tfs.fsyncSync(writer);
        StorageUtil.closeChannels(tfs.closeSync(writer));

        AsyncSegmentFile reader = openTfsSeg(dir, false);

        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            ByteBuf buf = tfs.readSync(reader, CHUNK_SIZE, (long) i * CHUNK_SIZE);
            buf.release();
        }

        long start = System.nanoTime();
        for (int i = 0; i < TOTAL_ITERATIONS; i++) {
            ByteBuf buf = tfs.readSync(reader, CHUNK_SIZE, (long) i * CHUNK_SIZE);
            buf.release();
        }
        long elapsed = System.nanoTime() - start;

        StorageUtil.closeChannels(tfs.closeSync(reader));
        return (double) TOTAL_ITERATIONS * CHUNK_SIZE / (elapsed / 1_000_000_000.0) / (1024 * 1024);
    }

    private double benchAsyncSegmentFileRead_Direct() throws Exception {
        String p = path("bench_sf_read_direct");
        try (FileChannel ch = FileChannel.open(Paths.get(p),
                StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            ByteBuffer buf = ByteBuffer.allocateDirect(CHUNK_SIZE);
            for (int i = 0; i < TOTAL_ITERATIONS; i++) {
                buf.clear();
                while (buf.hasRemaining()) ch.write(buf);
            }
            ch.force(true);
        }

        try (FileChannel ch = FileChannel.open(Paths.get(p), StandardOpenOption.READ)) {
            ByteBuffer buf = ByteBuffer.allocateDirect(CHUNK_SIZE);

            for (int i = 0; i < WARMUP_ITERATIONS; i++) {
                buf.clear();
                long pos = (long) i * CHUNK_SIZE;
                while (buf.hasRemaining()) {
                    int n = ch.read(buf, pos + buf.position());
                    if (n < 0) break;
                }
            }

            long start = System.nanoTime();
            for (int i = 0; i < TOTAL_ITERATIONS; i++) {
                buf.clear();
                long pos = (long) i * CHUNK_SIZE;
                while (buf.hasRemaining()) {
                    int n = ch.read(buf, pos + buf.position());
                    if (n < 0) break;
                }
            }
            long elapsed = System.nanoTime() - start;

            return (double) TOTAL_ITERATIONS * CHUNK_SIZE / (elapsed / 1_000_000_000.0) / (1024 * 1024);
        }
    }

    private double benchAsyncSegmentFileRead_TFSOnIoThread() throws Exception {
        String dir = path("bench_sf_read_tfs_io");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile writer = openTfsSeg(dir, true);
        for (int i = 0; i < TOTAL_ITERATIONS; i++) {
            writeSeg(writer, bufOf(new byte[CHUNK_SIZE]));
        }
        tfs.fsyncSync(writer);
        StorageUtil.closeChannels(tfs.closeSync(writer));

        AsyncSegmentFile reader = openTfsSeg(dir, false);

        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            long offset = (long) i * CHUNK_SIZE;
            ByteBuf buf = CompletableFuture.supplyAsync(() -> tfs.readSync(reader, CHUNK_SIZE, offset), ioExecutor).get();
            buf.release();
        }

        long start = System.nanoTime();
        for (int i = 0; i < TOTAL_ITERATIONS; i++) {
            long offset = (long) i * CHUNK_SIZE;
            ByteBuf buf = CompletableFuture.supplyAsync(() -> tfs.readSync(reader, CHUNK_SIZE, offset), ioExecutor).get();
            buf.release();
        }
        long elapsed = System.nanoTime() - start;

        StorageUtil.closeChannels(tfs.closeSync(reader));
        return (double) TOTAL_ITERATIONS * CHUNK_SIZE / (elapsed / 1_000_000_000.0) / (1024 * 1024);
    }

    private double benchAsyncSegmentFileRead_DirectOnIoThread() throws Exception {
        String p = path("bench_sf_read_direct_io");
        try (FileChannel ch = FileChannel.open(Paths.get(p),
                StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            ByteBuffer buf = ByteBuffer.allocateDirect(CHUNK_SIZE);
            for (int i = 0; i < TOTAL_ITERATIONS; i++) {
                buf.clear();
                while (buf.hasRemaining()) ch.write(buf);
            }
            ch.force(true);
        }

        try (FileChannel ch = FileChannel.open(Paths.get(p), StandardOpenOption.READ)) {
            ByteBuffer buf = ByteBuffer.allocateDirect(CHUNK_SIZE);

            for (int i = 0; i < WARMUP_ITERATIONS; i++) {
                long pos = (long) i * CHUNK_SIZE;
                CompletableFuture.supplyAsync(() -> {
                    buf.clear();
                    try { while (buf.hasRemaining()) { int n = ch.read(buf, pos + buf.position()); if (n < 0) break; } }
                    catch (Exception e) { throw new RuntimeException(e); }
                    return null;
                }, ioExecutor).get();
            }

            long start = System.nanoTime();
            for (int i = 0; i < TOTAL_ITERATIONS; i++) {
                long pos = (long) i * CHUNK_SIZE;
                CompletableFuture.supplyAsync(() -> {
                    buf.clear();
                    try { while (buf.hasRemaining()) { int n = ch.read(buf, pos + buf.position()); if (n < 0) break; } }
                    catch (Exception e) { throw new RuntimeException(e); }
                    return null;
                }, ioExecutor).get();
            }
            long elapsed = System.nanoTime() - start;

            return (double) TOTAL_ITERATIONS * CHUNK_SIZE / (elapsed / 1_000_000_000.0) / (1024 * 1024);
        }
    }

    // =========================================================================
    // Main — run all benchmarks and print consolidated results
    // =========================================================================

    interface BenchTask {
        String name();
        double run() throws Exception;
    }

    public static void main(String[] args) throws Exception {
        StorageBenchmark bench = new StorageBenchmark();
        bench.setUp();
        try {
            runAll(bench);
        } finally {
            bench.tearDown();
        }
    }

    private static void runAll(StorageBenchmark bench) throws Exception {
        List<BenchTask> tasks = new ArrayList<>();

        // --- AsyncFile Write ---
        tasks.add(new BenchTask() {
            public String name() { return "AsyncFile    Write  TailCacheFileSystem"; }
            public double run() throws Exception { return bench.benchAsyncFileWrite_TailCache(); }
        });
        tasks.add(new BenchTask() {
            public String name() { return "AsyncFile    Write  AsyncTFSBasedFileSystem"; }
            public double run() throws Exception { return bench.benchAsyncFileWrite_TFS(); }
        });
        tasks.add(new BenchTask() {
            public String name() { return "AsyncFile    Write  TFS on IoThread"; }
            public double run() throws Exception { return bench.benchAsyncFileWrite_TFSOnIoThread(); }
        });
        tasks.add(new BenchTask() {
            public String name() { return "AsyncFile    Write  DirectFileChannel"; }
            public double run() throws Exception { return bench.benchAsyncFileWrite_Direct(); }
        });
        tasks.add(new BenchTask() {
            public String name() { return "AsyncFile    Write  Direct on IoThread"; }
            public double run() throws Exception { return bench.benchAsyncFileWrite_DirectOnIoThread(); }
        });

        // --- AsyncFile Read ---
        tasks.add(new BenchTask() {
            public String name() { return "AsyncFile    Read   TailCacheFileSystem(miss)"; }
            public double run() throws Exception { return bench.benchAsyncFileRead_TailCache(); }
        });
        tasks.add(new BenchTask() {
            public String name() { return "AsyncFile    Read   AsyncTFSBasedFileSystem"; }
            public double run() throws Exception { return bench.benchAsyncFileRead_TFS(); }
        });
        tasks.add(new BenchTask() {
            public String name() { return "AsyncFile    Read   TFS on IoThread"; }
            public double run() throws Exception { return bench.benchAsyncFileRead_TFSOnIoThread(); }
        });
        tasks.add(new BenchTask() {
            public String name() { return "AsyncFile    Read   DirectFileChannel"; }
            public double run() throws Exception { return bench.benchAsyncFileRead_Direct(); }
        });
        tasks.add(new BenchTask() {
            public String name() { return "AsyncFile    Read   Direct on IoThread"; }
            public double run() throws Exception { return bench.benchAsyncFileRead_DirectOnIoThread(); }
        });

        // --- AsyncSegmentFile Write ---
        tasks.add(new BenchTask() {
            public String name() { return "SegmentFile  Write  TailCacheFileSystem"; }
            public double run() throws Exception { return bench.benchAsyncSegmentFileWrite_TailCache(); }
        });
        tasks.add(new BenchTask() {
            public String name() { return "SegmentFile  Write  AsyncTFSBasedFileSystem"; }
            public double run() throws Exception { return bench.benchAsyncSegmentFileWrite_TFS(); }
        });
        tasks.add(new BenchTask() {
            public String name() { return "SegmentFile  Write  TFS on IoThread"; }
            public double run() throws Exception { return bench.benchAsyncSegmentFileWrite_TFSOnIoThread(); }
        });
        tasks.add(new BenchTask() {
            public String name() { return "SegmentFile  Write  DirectFileChannel"; }
            public double run() throws Exception { return bench.benchAsyncSegmentFileWrite_Direct(); }
        });
        tasks.add(new BenchTask() {
            public String name() { return "SegmentFile  Write  Direct on IoThread"; }
            public double run() throws Exception { return bench.benchAsyncSegmentFileWrite_DirectOnIoThread(); }
        });

        // --- AsyncSegmentFile Read ---
        tasks.add(new BenchTask() {
            public String name() { return "SegmentFile  Read   TailCacheFileSystem(miss)"; }
            public double run() throws Exception { return bench.benchAsyncSegmentFileRead_TailCache(); }
        });
        tasks.add(new BenchTask() {
            public String name() { return "SegmentFile  Read   AsyncTFSBasedFileSystem"; }
            public double run() throws Exception { return bench.benchAsyncSegmentFileRead_TFS(); }
        });
        tasks.add(new BenchTask() {
            public String name() { return "SegmentFile  Read   TFS on IoThread"; }
            public double run() throws Exception { return bench.benchAsyncSegmentFileRead_TFSOnIoThread(); }
        });
        tasks.add(new BenchTask() {
            public String name() { return "SegmentFile  Read   DirectFileChannel"; }
            public double run() throws Exception { return bench.benchAsyncSegmentFileRead_Direct(); }
        });
        tasks.add(new BenchTask() {
            public String name() { return "SegmentFile  Read   Direct on IoThread"; }
            public double run() throws Exception { return bench.benchAsyncSegmentFileRead_DirectOnIoThread(); }
        });

        // Run all and collect results
        Map<String, Double> results = new LinkedHashMap<>();
        System.out.printf("Running benchmarks: %d iterations x %dKB per write, %d warmup iterations%n",
                TOTAL_ITERATIONS, CHUNK_SIZE / 1024, WARMUP_ITERATIONS);
        System.out.println("=".repeat(70));

        for (BenchTask task : tasks) {
            System.out.printf("  %-45s", task.name() + " ...");
            System.out.flush();
            try {
                double mbps = task.run();
                results.put(task.name(), mbps);
                System.out.printf(" %8.2f MB/s%n", mbps);
            } catch (Exception e) {
                results.put(task.name(), -1.0);
                System.out.printf(" FAILED: %s%n", e.getMessage());
            }
        }

        // Print summary table
        System.out.println();
        System.out.println("=".repeat(70));
        System.out.println("Summary (MB/s, higher is better):");
        System.out.println("-".repeat(70));
        System.out.printf("  %-45s %10s%n", "Scenario", "MB/s");
        System.out.println("-".repeat(70));

        String[] groups = {"AsyncFile    Write", "AsyncFile    Read",
                           "SegmentFile  Write", "SegmentFile  Read"};
        for (String group : groups) {
            System.out.printf("  [%s]%n", group);
            double baseline = -1;
            for (Map.Entry<String, Double> e : results.entrySet()) {
                if (e.getKey().contains(group)) {
                    double mbps = e.getValue();
                    String label = e.getKey().replace(group, "").trim();
                    if (label.contains("Direct")) {
                        baseline = mbps;
                    }
                    System.out.printf("    %-41s %8.2f%n", label, mbps);
                }
            }
            // Print overhead ratios relative to Direct baseline
            if (baseline > 0) {
                for (Map.Entry<String, Double> e : results.entrySet()) {
                    if (e.getKey().contains(group) && !e.getKey().contains("Direct")) {
                        double mbps = e.getValue();
                        if (mbps > 0) {
                            String label = e.getKey().replace(group, "").trim();
                            System.out.printf("      %s overhead: %.1f%% of direct%n",
                                    label, (1.0 - mbps / baseline) * 100);
                        }
                    }
                }
            }
            System.out.println();
        }
        System.out.println("=".repeat(70));
    }
}
