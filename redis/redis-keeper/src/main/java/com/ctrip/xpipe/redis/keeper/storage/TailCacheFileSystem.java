package com.ctrip.xpipe.redis.keeper.storage;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;

import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.ctrip.xpipe.redis.keeper.storage.AbstractStorageFile.CacheMode;
import com.ctrip.xpipe.redis.keeper.storage.TailCacheFileSystemConfig.BackingFsMode;
import com.ctrip.xpipe.tuple.Pair;

import io.netty.buffer.Unpooled;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * No idle/background flush for residual cache below writeBatchBytes when writes stop:
 * streaming workloads keep writing; non-streaming callers are expected to close() (or fsync())
 * to seal. In current use, small loss in extreme cases is recoverable externally.
 * <p>
 * If idle/async flush is added later, a workable approach is to tighten in-flight ownership:
 * change await into a locked await-and-register (wait prior IO, then claim the slot under the
 * same lock), and change register into bind-future (attach the real IO future to the claimed
 * slot). That removes the await/register TOCTOU that would otherwise race with write/close.
 * A claimed placeholder must count as in-flight; after claim, always bind the real future or
 * release the claim, including on failure paths.
 */
public class TailCacheFileSystem implements AsyncFileSystem {

    private static final Logger logger = LoggerFactory.getLogger(TailCacheFileSystem.class);
    private static final int LOCK_STRIPES = 32;

    private final AsyncFileSystem delegate;
    private volatile boolean readPreferCache;
    private volatile boolean transferPreferCache;
    private volatile BackingFsMode backingFsMode;
    private volatile long maxCacheSizeBytes;
    private volatile long maxCacheSizePerFileBytes;
    private volatile int minRetainChunks;
    private volatile long expectedMinRetentionMs;
    private volatile double lowWatermarkRatio;
    private volatile double highWatermarkRatio;
    private volatile long evictScanIntervalMs;
    private volatile double evictBandWidthRatio;
    private volatile int evictBandCount;
    private volatile double maxEvictRatioPerWrite;
    private volatile long asyncEvictIdleDeltaMs;
    private final long chunkSize;
    private volatile int preloadChunkThreshold;
    private volatile long ioWaitTimeoutMs;
    private volatile long writeBatchBytes;
    private volatile int maxWriteChunkThreshold;
    private volatile int eioRetryMaxAttempts;
    private final ExecutorService ioExecutor;
    private final CacheMemoryTracker memoryTracker = new CacheMemoryTracker();
    private final ScheduledExecutorService evictExecutor;
    private final AtomicBoolean shuttingDown = new AtomicBoolean();
    private volatile Map<String, Double> fileEvictRatios = Collections.emptyMap();

    private final ConcurrentHashMap<String, FileCacheEntry> fileCacheEntries = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, SegmentFileCacheEntry> segmentCacheEntries = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CompletableFuture<?>> inFlightIo = new ConcurrentHashMap<>();

    private final Object[] locks = new Object[LOCK_STRIPES];

    public TailCacheFileSystem(AsyncFileSystem delegate, TailCacheFileSystemConfig config, ExecutorService ioExecutor) {
        this.delegate = delegate;
        this.readPreferCache = config.isReadPreferCache();
        this.transferPreferCache = config.isTransferPreferCache();
        this.backingFsMode = config.getBackingFsMode();
        this.maxCacheSizeBytes = config.getMaxCacheSizeBytes();
        this.maxCacheSizePerFileBytes = config.getMaxCacheSizePerFileBytes();
        this.minRetainChunks = config.getMinRetainChunks();
        this.expectedMinRetentionMs = config.getExpectedMinRetentionMs();
        this.lowWatermarkRatio = config.getLowWatermarkRatio();
        this.highWatermarkRatio = config.getHighWatermarkRatio();
        this.evictScanIntervalMs = config.getEvictScanIntervalMs();
        this.evictBandWidthRatio = config.getEvictBandWidthRatio();
        this.evictBandCount = config.getEvictBandCount();
        this.maxEvictRatioPerWrite = config.getMaxEvictRatioPerWrite();
        this.asyncEvictIdleDeltaMs = config.getAsyncEvictIdleDeltaMs();
        this.chunkSize = config.getChunkSize();
        this.preloadChunkThreshold = config.getPreloadChunkThreshold();
        this.ioWaitTimeoutMs = config.getIoWaitTimeoutMs();
        this.writeBatchBytes = config.getWriteBatchBytes();
        this.maxWriteChunkThreshold = config.getMaxWriteChunkThreshold();
        this.eioRetryMaxAttempts = config.getEioRetryMaxAttempts();
        this.ioExecutor = ioExecutor;
        for (int i = 0; i < LOCK_STRIPES; i++) locks[i] = new Object();
        this.evictExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "tail-cache-evict-scanner");
            thread.setDaemon(true);
            return thread;
        });
        scheduleNextEvictScan();
    }

    public boolean isReadPreferCache() {
        return readPreferCache;
    }

    public void setReadPreferCache(boolean readPreferCache) {
        this.readPreferCache = readPreferCache;
    }

    public boolean isTransferPreferCache() {
        return transferPreferCache;
    }

    public void setTransferPreferCache(boolean transferPreferCache) {
        this.transferPreferCache = transferPreferCache;
    }

    public BackingFsMode getBackingFsMode() {
        return backingFsMode;
    }

    public void setBackingFsMode(BackingFsMode backingFsMode) {
        this.backingFsMode = backingFsMode;
    }

    public long getMaxCacheSizeBytes() {
        return maxCacheSizeBytes;
    }

    public void setMaxCacheSizeBytes(long maxCacheSizeBytes) {
        TailCacheFileSystemConfig.validateMaxCacheSizeBytes(maxCacheSizeBytes);
        this.maxCacheSizeBytes = maxCacheSizeBytes;
    }

    public long getMaxCacheSizePerFileBytes() {
        return maxCacheSizePerFileBytes;
    }

    public int getMinRetainChunks() {
        return minRetainChunks;
    }

    public long getChunkSize() {
        return chunkSize;
    }

    public void setPerFileCacheLimits(long maxCacheSizePerFileBytes, int minRetainChunks) {
        TailCacheFileSystemConfig.validatePerFileCacheLimits(maxCacheSizePerFileBytes, minRetainChunks, chunkSize);
        this.maxCacheSizePerFileBytes = maxCacheSizePerFileBytes;
        this.minRetainChunks = minRetainChunks;
    }

    public long getExpectedMinRetentionMs() {
        return expectedMinRetentionMs;
    }

    public void setExpectedMinRetentionMs(long expectedMinRetentionMs) {
        TailCacheFileSystemConfig.validateExpectedMinRetentionMs(expectedMinRetentionMs);
        this.expectedMinRetentionMs = expectedMinRetentionMs;
    }

    public double getLowWatermarkRatio() {
        return lowWatermarkRatio;
    }

    public double getHighWatermarkRatio() {
        return highWatermarkRatio;
    }

    public void setWatermarkRatios(double lowWatermarkRatio, double highWatermarkRatio) {
        TailCacheFileSystemConfig.validateWatermarkRatios(lowWatermarkRatio, highWatermarkRatio);
        this.lowWatermarkRatio = lowWatermarkRatio;
        this.highWatermarkRatio = highWatermarkRatio;
    }

    public long getEvictScanIntervalMs() {
        return evictScanIntervalMs;
    }

    public void setEvictScanIntervalMs(long evictScanIntervalMs) {
        TailCacheFileSystemConfig.validateEvictScanIntervalMs(evictScanIntervalMs);
        this.evictScanIntervalMs = evictScanIntervalMs;
    }

    public double getEvictBandWidthRatio() {
        return evictBandWidthRatio;
    }

    public int getEvictBandCount() {
        return evictBandCount;
    }

    public void setEvictBands(double evictBandWidthRatio, int evictBandCount) {
        TailCacheFileSystemConfig.validateEvictBands(evictBandWidthRatio, evictBandCount);
        this.evictBandWidthRatio = evictBandWidthRatio;
        this.evictBandCount = evictBandCount;
    }

    public double getMaxEvictRatioPerWrite() {
        return maxEvictRatioPerWrite;
    }

    public void setMaxEvictRatioPerWrite(double maxEvictRatioPerWrite) {
        TailCacheFileSystemConfig.validateMaxEvictRatioPerWrite(maxEvictRatioPerWrite);
        this.maxEvictRatioPerWrite = maxEvictRatioPerWrite;
    }

    public long getAsyncEvictIdleDeltaMs() {
        return asyncEvictIdleDeltaMs;
    }

    public void setAsyncEvictIdleDeltaMs(long asyncEvictIdleDeltaMs) {
        TailCacheFileSystemConfig.validateAsyncEvictIdleDeltaMs(asyncEvictIdleDeltaMs);
        this.asyncEvictIdleDeltaMs = asyncEvictIdleDeltaMs;
    }

    public long getGlobalCommittedBytes() {
        return memoryTracker.committedBytes();
    }

    private CacheMode resolveFileCacheMode(boolean atomicReplace, CacheMode override) {
        if (override != null) {
            if (atomicReplace && override == CacheMode.TAIL_CACHE) {
                throw new IllegalArgumentException("TAIL_CACHE is not supported for atomicReplace");
            }
            return override;
        }
        return atomicReplace ? CacheMode.FULL_CACHE : CacheMode.TAIL_CACHE;
    }

    private CacheMode resolveSegmentCacheMode(CacheMode override) {
        if (override != null) {
            if (override == CacheMode.FULL_CACHE) {
                throw new IllegalArgumentException("FULL_CACHE is not supported for segment files");
            }
            return override;
        }
        return CacheMode.TAIL_CACHE;
    }

    public int getPreloadChunkThreshold() {
        return preloadChunkThreshold;
    }

    public void setPreloadChunkThreshold(int preloadChunkThreshold) {
        TailCacheFileSystemConfig.validatePreloadChunkThreshold(preloadChunkThreshold);
        this.preloadChunkThreshold = preloadChunkThreshold;
    }

    public long getIoWaitTimeoutMs() {
        return ioWaitTimeoutMs;
    }

    public void setIoWaitTimeoutMs(long ioWaitTimeoutMs) {
        TailCacheFileSystemConfig.validateIoWaitTimeoutMs(ioWaitTimeoutMs);
        this.ioWaitTimeoutMs = ioWaitTimeoutMs;
    }

    public long getWriteBatchBytes() {
        return writeBatchBytes;
    }

    public void setWriteBatchBytes(long writeBatchBytes) {
        TailCacheFileSystemConfig.validateWriteBatchBytes(writeBatchBytes);
        this.writeBatchBytes = writeBatchBytes;
    }

    public int getMaxWriteChunkThreshold() {
        return maxWriteChunkThreshold;
    }

    public void setMaxWriteChunkThreshold(int maxWriteChunkThreshold) {
        TailCacheFileSystemConfig.validateMaxWriteChunkThreshold(maxWriteChunkThreshold);
        this.maxWriteChunkThreshold = maxWriteChunkThreshold;
    }

    public int getEioRetryMaxAttempts() {
        return eioRetryMaxAttempts;
    }

    public void setEioRetryMaxAttempts(int eioRetryMaxAttempts) {
        TailCacheFileSystemConfig.validateEioRetryMaxAttempts(eioRetryMaxAttempts);
        this.eioRetryMaxAttempts = eioRetryMaxAttempts;
    }

    @Override
    public void shutdown() {
        shuttingDown.set(true);
        evictExecutor.shutdownNow();
        delegate.shutdown();
    }

    private void scheduleNextEvictScan() {
        if (shuttingDown.get()) return;
        evictExecutor.schedule(this::runEvictScan, evictScanIntervalMs, TimeUnit.MILLISECONDS);
    }

    private void runEvictScan() {
        try {
            long committed = memoryTracker.committedBytes();
            double ratio = (double) committed / maxCacheSizeBytes;
            if (ratio < lowWatermarkRatio) {
                fileEvictRatios = Collections.emptyMap();
                return;
            }

            boolean shouldAsyncEvict = ratio > (lowWatermarkRatio + highWatermarkRatio) / 2;
            long now = System.nanoTime();
            List<Pair<String, Long>> candidates = new ArrayList<>();
            fileCacheEntries.forEach((key, entry) -> {
                long bytes = entry.cacheSizeBytes();
                if (entry.evictable && bytes > 0 && entry.chunks.size() > minRetainChunks) {
                    candidates.add(Pair.of(key, bytes));
                    if (shouldAsyncEvict) {
                        tryAsyncEvict(key, entry, now);
                    }
                }
            });
            segmentCacheEntries.forEach((key, entry) -> {
                long bytes = entry.cacheSizeBytes();
                if (bytes > 0 && entry.chunks.size() > minRetainChunks) {
                    candidates.add(Pair.of(key, bytes));
                    if (shouldAsyncEvict) {
                        tryAsyncEvict(key, entry, now);
                    }
                }
            });
            candidates.sort(Comparator.comparingLong((Pair<String, Long> p) -> p.getValue()).reversed());
            Map<String, Double> ratios = new HashMap<>();
            long sumBefore = 0;
            double bandWidthRatio = evictBandWidthRatio;
            int bandCount = evictBandCount;
            double[] evictRatios = computeEvictRatios(bandCount);
            for (Pair<String, Long> candidate : candidates) {
                double startRatio = (double) sumBefore / committed;
                int band = (int) (startRatio / bandWidthRatio);
                if (band >= bandCount) break;
                ratios.put(candidate.getKey(), evictRatios[band]);
                sumBefore += candidate.getValue();
            }
            fileEvictRatios = Collections.unmodifiableMap(ratios);
        } catch (Throwable t) {
            logger.warn("tail cache evict scan failed", t);
        } finally {
            scheduleNextEvictScan();
        }
    }

    private void tryAsyncEvict(String fileKey, FileCacheEntry entry, long nowNanos) {
        long lastChunkIndex = (entry.cacheEndOffset - 1) / chunkSize;
        CacheChunk lastChunk = entry.chunks.get(lastChunkIndex);
        if (lastChunk == null) return;
        long lastAppendNanos = lastChunk.lastAppendNanos;
        if (lastAppendNanos == 0) return; // Not yet written
        long threshold = nowNanos - TimeUnit.MILLISECONDS.toNanos(expectedMinRetentionMs + asyncEvictIdleDeltaMs);
        if (lastAppendNanos > threshold) return;

        // Check durableFsOffset > first chunk end
        long durableFsOffset = entry.writtenToFsOffset - entry.pendingFsyncBytes;
        long firstChunkEnd = (entry.cacheStartOffset / chunkSize + 1) * chunkSize;
        if (durableFsOffset <= firstChunkEnd) return;

        synchronized (entry) {
            if (entry.isInitialized()) {
                evictTailBeforeAppend(fileKey, entry, 0, nowNanos);
            }
        }
    }

    private double[] computeEvictRatios(int bandCount) {
        double maxEvictRatio = maxEvictRatioPerWrite;
        double maxRatio = maxEvictRatio / 2;
        double minRatio = maxEvictRatio / 4;
        double[] ratios = new double[bandCount];
        if (bandCount == 1) {
            ratios[0] = maxRatio;
        } else {
            double step = (maxRatio - minRatio) / (bandCount - 1);
            for (int i = 0; i < bandCount; i++) {
                ratios[i] = maxRatio - i * step;
            }
        }
        return ratios;
    }

    // ---- lock helpers ----

    private Object lockFor(String key) {
        return locks[(key.hashCode() & 0x7fffffff) % LOCK_STRIPES];
    }

    private boolean hasInFlightIo(String id) {
        return inFlightIo.containsKey(id);
    }

    private void awaitInFlightIo(String id) {
        CompletableFuture<?> f = inFlightIo.get(id);
        if (f == null) {
            return;
        }
        try {
            if (ioWaitTimeoutMs > 0) {
                f.get(ioWaitTimeoutMs, TimeUnit.MILLISECONDS);
            } else {
                f.get();
            }
        } catch (TimeoutException | InterruptedException e) {
            throw new OperationNotExecutedException(id, e);
        } catch (ExecutionException e) {
            logger.warn("prior IO failed for {}, ignoring during wait", id, e);
        }
    }

    private void registerInFlight(String id, CompletableFuture<?> op) {
        inFlightIo.put(id, op);
        op.whenComplete((r, e) -> inFlightIo.remove(id, op));
    }

    // require file inflight io to be completed before calling this
    private void flushPendingWriteAndAwait(AbstractStorageFile file,
            java.util.function.Function<ByteBuf, Long> fsWrite,
            Runnable fsFsync, boolean failIfStillDirty) {
        if (!file.canWrite() || file.getCacheEntry() == null) {
            return;
        }
        FileCacheEntry entry = file.getCacheEntry();
        if (!entry.isInitialized()) {
            return;
        }
        boolean cacheDirty = file.atomicReplace
                ? entry.cacheGen != entry.writtenGen
                : entry.cacheEndOffset > entry.writtenToFsOffset;
        boolean fsyncDirty = entry.pendingFsyncBytes > 0;
        if (!cacheDirty && !fsyncDirty) {
            return;
        }
        final String id = file.getKey();
        final ByteBuf writeBuf;
        final long ioGen;
        if (file.atomicReplace) {
            Pair<Long, ByteBuf> atomic = getPendingAtomicWriteBufAfterInFlight(entry);
            writeBuf = atomic.getValue();
            ioGen = atomic.getKey();
        } else {
            writeBuf = buildWriteBufFromCache(entry, Long.MAX_VALUE);
            ioGen = 0;
        }
        final boolean hasWriteData = writeBuf.isReadable();
        if (!hasWriteData && !fsyncDirty) {
            writeBuf.release();
            return;
        }
        CompletableFuture<Long> flushFuture = StorageUtil.supply(ioExecutor, () -> {
            if (file.cacheClosed) {
                writeBuf.release();
                throw new IllegalStateException("file cache is closed: " + file.getKey());
            }
            long written = 0;
            if (hasWriteData) {
                written = fsWrite.apply(writeBuf);
            } else {
                writeBuf.release();
            }
            synchronized (entry) {
                if (file.atomicReplace) {
                    if (ioGen > entry.writtenGen) {
                        entry.writtenGen = ioGen;
                    }
                    entry.writtenToFsOffset = written;
                } else {
                    entry.writtenToFsOffset += written;
                }
                entry.pendingFsyncBytes = file.pendingFsyncBytes;
            }
            fsFsync.run();
            entry.pendingFsyncBytes = file.pendingFsyncBytes;
            return written;
        }, writeBuf);
        registerInFlight(id, flushFuture);
        awaitInFlightIo(id);
        Throwable flushError = null;
        boolean retryableFailure = false;
        try {
            flushFuture.get(1, TimeUnit.NANOSECONDS);
        } catch (ExecutionException e) {
            flushError = e.getCause();
            retryableFailure = flushError instanceof EIOException;
        } catch (TimeoutException e) {
            flushError = e;
            retryableFailure = true;
        } catch (Exception e) {
            flushError = e;
        }

        boolean stillDirty = file.atomicReplace
                ? entry.cacheGen != entry.writtenGen
                : entry.cacheEndOffset > entry.writtenToFsOffset
                        || entry.pendingFsyncBytes > 0;
        if (stillDirty) {
            if (retryableFailure) {
                throw new OperationNotExecutedException(id, flushError);
            }
            if (failIfStillDirty) {
                throw new IllegalStateException(
                        "unflushed cache remains after drain for " + id + "; refusing write to avoid corruption",
                        flushError);
            }
            long remainingBytes = Math.max(0, entry.cacheEndOffset - entry.writtenToFsOffset);
            if (flushError != null) {
                logger.error("data lost for {}, still has {} bytes not flushed after flush, cacheGen={}, writtenGen={}",
                        id, remainingBytes, entry.cacheGen, entry.writtenGen, flushError);
            } else {
                logger.error("data lost for {}, still has {} bytes not flushed after flush with non error, cacheGen={}, writtenGen={}",
                        id, remainingBytes, entry.cacheGen, entry.writtenGen);
            }
            return;
        }
        if (flushError != null) {
            logger.error("flush finished with error for {}, unexpected", id, flushError);
        }
    }

    // require file inflight io to be completed before calling this
    private void asyncFileFlushPendingWriteAndAwait(AsyncFile file, boolean failIfStillDirty) {
        flushPendingWriteAndAwait(file,
                writeBuf -> executeWithEioRetry(file, () -> delegate.writeSync(file, writeBuf)),
                () -> executeWithEioRetry(file, () -> {
                    delegate.fsyncSync(file);
                    return null;
                }),
                failIfStillDirty);
    }

    // require segment inflight io to be completed before calling this
    private void segmentFlushPendingWriteAndAwait(AsyncSegmentFile file, boolean failIfStillDirty) {
        flushPendingWriteAndAwait(file,
                writeBuf -> executeWithEioRetry(file, () -> delegate.writeSync(file, writeBuf)),
                () -> executeWithEioRetry(file, () -> {
                    delegate.fsyncSync(file);
                    return null;
                }),
                failIfStillDirty);
        for (AsyncIndexFile indexFile : file.currentIndexFiles.values()) {
            awaitInFlightIo(indexFile.getKey());
            asyncFileFlushPendingWriteAndAwait(indexFile, failIfStillDirty);
        }
    }

    private <F extends AbstractStorageFile, T> T executeWithEioRetry(F file,
            java.util.function.Supplier<Long> getCurrentWrittenToFsOffset,
            java.util.function.Supplier<T> ioAction) {
        int maxAttempts = eioRetryMaxAttempts;
        try {
            return ioAction.get();
        } catch (RuntimeException e) {
            if (!(e instanceof EIOException)) {
                throw e;
            }
            logger.warn("io action got EIO for {}, retrying reopen up to {} times", file.getKey(), maxAttempts, e);
            for (int attempt = 1; attempt <= maxAttempts; attempt++) {
                try {
                    file.reopenCurrentChannel();
                    resetWrittenToFsOffsetIfNeeded(file, getCurrentWrittenToFsOffset);
                    break;
                } catch (Exception retryError) {
                    logger.error("retry attempt {}/{} failed for {}", attempt, maxAttempts, file.getKey(), retryError);
                }
            }
            throw e;
        }
    }

    private <T> T executeWithEioRetry(AsyncFile file, java.util.function.Supplier<T> ioAction) {
        return executeWithEioRetry(file, () -> delegate.sizeSync(file), ioAction);
    }

    private <T> T executeWithEioRetry(AsyncSegmentFile file, java.util.function.Supplier<T> ioAction) {
        return executeWithEioRetry(file, () -> segmentExclusiveEndOffset(file), ioAction);
    }

    private <F extends AbstractStorageFile> void resetWrittenToFsOffsetIfNeeded(F file,
            java.util.function.Supplier<Long> getCurrentWrittenToFsOffset) throws Exception {
        if (!file.canWrite()) {
            return;
        }
        FileCacheEntry entry = file.getCacheEntry();
        if (entry == null) {
            return;
        }
        if (file.currentWriteChannel() == null) {
            return;
        }
        long fileSize = awaitIoCachePrep(file, getCurrentWrittenToFsOffset);
        entry.writtenToFsOffset = fileSize;
    }

    // ---- AsyncFile ----

    @Override
    public CompletableFuture<AsyncFile> open(String path, AbstractStorageFile.OpenMode openMode, boolean atomicReplace, boolean lenient, String tenant) {
        return StorageUtil.supply(ioExecutor, () -> openFileSync(path, openMode, atomicReplace, lenient, tenant, null));
    }

    public CompletableFuture<AsyncFile> open(String path, AbstractStorageFile.OpenMode openMode, boolean atomicReplace, boolean lenient, String tenant, CacheMode cacheMode) {
        return StorageUtil.supply(ioExecutor, () -> openFileSync(path, openMode, atomicReplace, lenient, tenant, cacheMode));
    }


    private AsyncFile openFileSync(String path, AbstractStorageFile.OpenMode openMode, boolean atomicReplace, boolean lenient, String tenant, CacheMode cacheModeOverride) {
        CacheMode cacheMode = resolveFileCacheMode(atomicReplace, cacheModeOverride);
        AbstractStorageFile.OpenMode effectiveOpenMode = openMode;
        if (openMode == AbstractStorageFile.OpenMode.WRITE && cacheMode == CacheMode.FULL_CACHE) {
            effectiveOpenMode = AbstractStorageFile.OpenMode.READ_WRITE;
        }
        AsyncFile file = delegate.openSync(path, effectiveOpenMode, atomicReplace, lenient, tenant);
        file.cacheMode = cacheMode;
        if (cacheMode != CacheMode.NO_CACHE) {
            String key = file.getKey();
            boolean write = file.canWrite();
            FileCacheEntry entry;
            boolean first;
            try {
                synchronized (lockFor(key)) {
                    entry = fileCacheEntries.computeIfAbsent(
                            key, k -> new FileCacheEntry(memoryTracker, cacheMode == CacheMode.TAIL_CACHE));
                    first = entry.retainEntry(write);
                }
            } catch (Throwable t) {
                logger.error("acquire file cache entry failed for {}, closing file", file.path, t);
                cleanupOpenFailedFile(file);
                throw t;
            }
            file.cacheEntry = entry;
            file.onCacheClose = () -> {
                FileCacheEntry shared = file.getCacheEntry();
                if (shared == null) {
                    return;
                }
                synchronized (lockFor(key)) {
                    if (shared.releaseEntry(write)) {
                        fileCacheEntries.remove(key, shared);
                    }
                }
            };
            initFileCache(file, first);
        }
        return file;
    }

    private void cleanupOpenFailedFile(AsyncFile file) {
        try {
            file.onCacheClose.run();
        } catch (Throwable t) {
            logger.error("failed to release cache entry for {}", file.path, t);
        }
        try {
            delegate.closeSync(file);
        } catch (Throwable t) {
            logger.error("closeSync failed during open cleanup for {}", file.path, t);
        }
    }

    private void cleanupOpenFailedSegment(AsyncSegmentFile file) {
        try {
            file.onCacheClose.run();
        } catch (Throwable t) {
            logger.error("failed to release segment cache entry for {}", file.getKey(), t);
        }
        try {
            delegate.closeSync(file);
        } catch (Throwable t) {
            logger.error("closeSync failed during open cleanup for {}", file.getKey(), t);
        }
    }

    private void initCache(FileCacheEntry entry, boolean first, Runnable init) {
        if (first) {
            init.run();
            entry.initDone.countDown();
        } else {
            try {
                entry.initDone.await();
            } catch (Throwable t) {
                logger.warn("await cache init failed", t);
            }
        }
    }

    private void initFileCache(AsyncFile file, boolean first) {
        initCache(file.cacheEntry, first, () -> {
            try {
                if (!useCache(file)) return;
                if (file.cacheMode == CacheMode.TAIL_CACHE && !file.cacheEntry.isInitialized()) {
                    initTailCacheSync(file, () -> delegate.sizeSync(file));
                }
                if (file.cacheMode == CacheMode.FULL_CACHE && !file.cacheEntry.isInitialized()) {
                    loadFullFileCache(file, 0L, false);
                }
            } catch (Exception e) {
                logger.warn("initFileCache failed for {}", file.getKey(), e);
            }
        });
    }

    private void initSegmentCache(AsyncSegmentFile file, boolean first) {
        initCache(file.cacheEntry, first, () -> {
            try {
                if (!useCache(file) || file.cacheEntry.isInitialized()) return;
                initTailCacheSync(file, () -> segmentExclusiveEndOffset(file));
            } catch (Exception e) {
                logger.warn("initSegmentCache failed for {}", file.getKey(), e);
            }
        });
    }

    private void initTailCacheSync(AbstractStorageFile file,
            java.util.function.Supplier<Long> endOffsetSupplier) {
        long endOffset = awaitIoCachePrep(file,
                () -> executeWithEioRetry(file, endOffsetSupplier, endOffsetSupplier));
        FileCacheEntry entry = file.cacheEntry;
        synchronized (entry) {
            if (entry.isInitialized()) {
                return;
            }
            entry.cacheStartOffset = endOffset;
            entry.cacheEndOffset = endOffset;
            entry.writtenToFsOffset = endOffset;
        }
    }

    private <T> T awaitIoCachePrep(AbstractStorageFile file, java.util.function.Supplier<T> task) {
        CompletableFuture<T> future = StorageUtil.supply(ioExecutor, () -> {
            StorageUtil.requireCacheOpen(file);
            return task.get();
        });
        try {
            if (ioWaitTimeoutMs > 0) {
                return future.get(ioWaitTimeoutMs, TimeUnit.MILLISECONDS);
            } else {
                return future.get();
            }
        } catch (Exception e) {
            throw new OperationNotExecutedException(file.getKey(), e);
        }
    }

    private void loadFullFileCache(AsyncFile file, long appendBytes, boolean memoryAllocateBlocking) {
        long reservedBytes = 0;
        ByteBuf fileData = null;
        Map<Long, ByteBuf> allocated = new HashMap<>();
        long actualSize;
        FileCacheEntry entry = file.cacheEntry;
        try {
            long initialSize = awaitIoCachePrep(file,
                    () -> executeWithEioRetry(file, () -> delegate.sizeSync(file)));
            long initialCapacity = file.atomicReplace
                    ? initialSize
                    : StorageUtil.chunkCapacityForBytes(initialSize + appendBytes, chunkSize);
            if (initialCapacity > maxCacheSizePerFileBytes) {
                entry.largeFile = true;
                throw new CacheFileTooLargeException(file.getKey(), initialCapacity);
            }
            if (memoryAllocateBlocking) {
                memoryTracker.reserve(initialCapacity, maxCacheSizeBytes, ioWaitTimeoutMs);
            } else if (!memoryTracker.tryReserve(initialCapacity, maxCacheSizeBytes)) {
                throw new CacheMemoryReserveException(initialCapacity, maxCacheSizeBytes, memoryTracker.committedBytes());
            }
            reservedBytes = initialCapacity;

            Pair<Boolean, ByteBuf> fullData = awaitIoCachePrep(file, () -> readFullData(file, initialSize));
            boolean aligned = fullData.getKey();
            fileData = fullData.getValue();
            actualSize = fileData.readableBytes();
            long actualCapacity = file.atomicReplace
                    ? actualSize
                    : StorageUtil.chunkCapacityForBytes(actualSize + appendBytes, chunkSize);
            if (actualCapacity > maxCacheSizePerFileBytes) {
                entry.largeFile = true;
                throw new CacheFileTooLargeException(file.getKey(), actualCapacity);
            }
            if (actualCapacity > reservedBytes) {
                long additionalBytes = actualCapacity - reservedBytes;
                if (memoryAllocateBlocking) {
                    memoryTracker.reserve(additionalBytes, maxCacheSizeBytes, ioWaitTimeoutMs);
                } else if (!memoryTracker.tryReserve(additionalBytes, maxCacheSizeBytes)) {
                    throw new CacheMemoryReserveException(additionalBytes, maxCacheSizeBytes, memoryTracker.committedBytes());
                }
                reservedBytes = actualCapacity;
            } else if (actualCapacity < reservedBytes) {
                memoryTracker.release(reservedBytes - actualCapacity);
                reservedBytes = actualCapacity;
            }
            if (file.atomicReplace) {
                allocated.put(0L, fileData.retain());
            } else if (aligned) {
                long dataChunks = fileData.capacity() / chunkSize;
                long totalChunks = actualCapacity / chunkSize;
                for (long i = 0; i < dataChunks; i++) {
                    allocated.put(i, fileData.retainedSlice((int) (i * chunkSize), (int) chunkSize));
                }
                for (long i = dataChunks; i < totalChunks; i++) {
                    ByteBuf chunk;
                    try {
                        chunk = StorageAllocator.ALLOC.directBuffer((int) chunkSize);
                    } catch (Throwable e) {
                        throw new CacheMemoryReserveException(chunkSize, e);
                    }
                    allocated.put(i, chunk);
                }
            } else {
                long offset = 0;
                while (fileData.isReadable()) {
                    long chunkIdx = offset / chunkSize;
                    int inChunk = (int) (offset % chunkSize);
                    if (!allocated.containsKey(chunkIdx)) {
                        ByteBuf chunk;
                        try {
                            chunk = StorageAllocator.ALLOC.directBuffer((int) chunkSize);
                        } catch (Throwable e) {
                            throw new CacheMemoryReserveException(chunkSize, e);
                        }
                        allocated.put(chunkIdx, chunk);
                    }
                    int length = (int) Math.min(chunkSize - inChunk, fileData.readableBytes());
                    ByteBuf chunk = allocated.get(chunkIdx);
                    chunk.writerIndex(inChunk);
                    chunk.writeBytes(fileData, length);
                    offset += length;
                }
                long dataChunks = (actualSize + chunkSize - 1) / chunkSize;
                long totalChunks = actualCapacity / chunkSize;
                for (long i = dataChunks; i < totalChunks; i++) {
                    ByteBuf chunk;
                    try {
                        chunk = StorageAllocator.ALLOC.directBuffer((int) chunkSize);
                    } catch (Throwable e) {
                        throw new CacheMemoryReserveException(chunkSize, e);
                    }
                    allocated.put(i, chunk);
                }
            }
            fileData.release();
        } catch (Throwable t) {
            for (ByteBuf chunk : allocated.values()) chunk.release();
            if (fileData != null) fileData.release();
            memoryTracker.release(reservedBytes);
            throw t;
        }

        synchronized (entry) {
            if (entry.isInitialized()) {
                for (ByteBuf chunk : allocated.values()) chunk.release();
                memoryTracker.release(reservedBytes);
                return;
            }
            for (Map.Entry<Long, ByteBuf> chunk : allocated.entrySet()) {
                entry.putChunk(chunk.getKey(), new CacheChunk(chunk.getValue()));
            }
            entry.cacheStartOffset = 0;
            entry.cacheEndOffset = actualSize;
            entry.writtenToFsOffset = actualSize;
        }
    }

    private Pair<Boolean, ByteBuf> readFullData(AsyncFile file, long fileSize) {
        boolean aligned = true;
        if (fileSize == 0) return new Pair<>(true, Unpooled.buffer(0));
        ByteBuf data;
        if (file.atomicReplace) {
            aligned = false;
            data = executeWithEioRetry(file,
                    () -> delegate.readSync(file, fileSize, 0, 0));
        } else if (fileSize <= preloadChunkThreshold * chunkSize) {
            // small file: aligned read — buffer capacity rounded up to chunkSize multiples,
            // so each chunk slice maps directly onto an aligned region without copying.
            data = executeWithEioRetry(file,
                    () -> delegate.readSync(file, fileSize, 0, chunkSize));
        } else {
            // large file: single read, copy into per-chunk buffers
            aligned = false;
            data = executeWithEioRetry(file,
                    () -> delegate.readSync(file, fileSize, 0, 0));
        }
        return new Pair<>(aligned, data);
    }

    @Override
    public CompletableFuture<Boolean> isFile(AsyncFile file) {
        return delegate.isFile(file);
    }

    @Override
    public CompletableFuture<Boolean> isDirectory(String path) {
        return delegate.isDirectory(path);
    }

    @Override
    public CompletableFuture<Long> lastModified(AsyncFile file) {
        return delegate.lastModified(file);
    }

    @Override
    public CompletableFuture<Void> position(AsyncFile file, long position) {
        if (!file.canRead()) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("position() requires read mode"));
        }
        try {
            StorageUtil.requireCacheOpen(file);
            delegate.positionSync(file, position);
            return CompletableFuture.completedFuture(null);
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }


    @Override
    public CompletableFuture<ByteBuf> read(AsyncFile file, long length, long offset) {
        return readInternal(file, length, offset, false,
                () -> executeWithEioRetry(file, () -> delegate.readSync(file, length, offset, 0)));
    }

    @Override
    public CompletableFuture<ByteBuf> read(AsyncFile file, long length) {
        long readOffset = file.position;
        return readInternal(file, length, 0, true,
                () -> executeWithEioRetry(file, () -> delegate.readSync(file, length, readOffset, 0)));
    }

    private CompletableFuture<ByteBuf> readInternal(AbstractStorageFile file, long length, long offset,
            boolean fromPosition, java.util.function.Supplier<ByteBuf> fsRead) {
        StorageUtil.requireCacheOpen(file);
        FileCacheEntry entry = file.getCacheEntry();
        long readOffset = fromPosition ? file.position : offset;
        ByteBuf cached = null;
        if (!preferDirectRead(file, entry, readOffset, readPreferCache)) {
            synchronized (entry) {
                if (readOffset >= entry.cacheStartOffset && entry.isInitialized()) {
                    cached = readWithCache(length, readOffset, entry, file.atomicReplace);
                }
            }
        }
        if (cached != null) {
            if (fromPosition) {
                file.position = readOffset + cached.readableBytes();
            }
            return CompletableFuture.completedFuture(cached);
        }

        return StorageUtil.supply(ioExecutor, () -> {
            StorageUtil.requireCacheOpen(file);
            ByteBuf buf = fsRead.get();
            if (fromPosition) {
                file.position = readOffset + buf.readableBytes();
            }
            return buf;
        });
    }


    // Must be called under synchronized(entry).
    boolean preferDirectRead(AbstractStorageFile file, FileCacheEntry entry, long offset, boolean preferCache) {
        if (file.cacheMode == CacheMode.NO_CACHE) return true;
        if (!(entry.isInitialized() && offset >= entry.cacheStartOffset)) return true;
        if (!preferCache || backingFsMode == BackingFsMode.NO_CACHE) {
            if (file.atomicReplace) {
                return entry.cacheGen == entry.writtenGen;
            }
            return offset < entry.writtenToFsOffset;
        }
        return false;
    }

    private java.util.List<ByteBuf> collectChunkSlices(FileCacheEntry entry, long offset, long end, boolean failOnMissingChunk) {
        long pos = offset;
        java.util.List<ByteBuf> slices = new java.util.ArrayList<>();
        while (pos < end) {
            long chunkIdx = pos / chunkSize;
            int inChunk = (int) (pos % chunkSize);
            CacheChunk cacheChunk = entry.chunks.get(chunkIdx);
            if (cacheChunk == null) {
                if (!failOnMissingChunk) {
                    break;
                }
                for (ByteBuf slice : slices) {
                    slice.release();
                }
                throw new CacheChunksNotContinuousException(
                        "cache chunks not continuous, missing chunk " + chunkIdx + " for range [" + offset + ", " + end + ")");
            }
            int length = (int) Math.min(chunkSize - inChunk, end - pos);
            slices.add(cacheChunk.buffer.retainedSlice(inChunk, length));
            pos += length;
        }
        return slices;
    }

    // Must be called under synchronized(entry).
    private java.util.List<ByteBuf> collectAtomicChunkSlice(
            FileCacheEntry entry, long offset, long end, boolean failOnMissingChunk) {
        java.util.List<ByteBuf> slices = new java.util.ArrayList<>(1);
        if (offset >= end) {
            return slices;
        }
        CacheChunk cacheChunk = entry.chunks.get(0L);
        int length = (int) (end - offset);
        if (cacheChunk == null || end > entry.cacheEndOffset) {
            if (failOnMissingChunk) {
                throw new CacheChunksNotContinuousException(
                        "atomic cache chunk 0 with size " + entry.cacheEndOffset
                                + " does not cover range [" + offset + ", " + end + ")");
            }
            return slices;
        }
        slices.add(cacheChunk.buffer.retainedSlice((int) offset, length));
        return slices;
    }

    // Must be called under synchronized(entry).
    private java.util.List<ByteBuf> collectCacheSlices(
            FileCacheEntry entry, long offset, long end, boolean failOnMissingChunk, boolean atomicReplace) {
        if (atomicReplace) {
            return collectAtomicChunkSlice(entry, offset, end, failOnMissingChunk);
        }
        return collectChunkSlices(entry, offset, end, failOnMissingChunk);
    }

    private ByteBuf readWithCache(
            long length, long offset, FileCacheEntry entry, boolean atomicReplace) {
        long end = Math.min(offset + length, entry.cacheEndOffset);
        java.util.List<ByteBuf> slices =
                collectCacheSlices(entry, offset, end, false, atomicReplace);
        CompositeByteBuf composite = StorageAllocator.ALLOC.compositeDirectBuffer();
        for (ByteBuf s : slices) {
            composite.addComponent(true, s);
        }
        return composite;
    }

    long transferToByCache(FileCacheEntry entry, long offset, long count,
            WritableByteChannel target, boolean atomicReplace) throws IOException {
        java.util.List<ByteBuf> slices;
        synchronized (entry) {
            long end = Math.min(offset + count, entry.cacheEndOffset);
            slices = collectCacheSlices(entry, offset, end, false, atomicReplace);
        }
        if (slices.isEmpty()) return 0;
        try {
            if (target instanceof GatheringByteChannel) {
                ByteBuffer[] nioBuffers = new ByteBuffer[slices.size()];
                for (int i = 0; i < slices.size(); i++) {
                    nioBuffers[i] = slices.get(i).nioBuffer();
                }
                return ((GatheringByteChannel) target).write(nioBuffers);
            }

            long transferred = 0;
            for (ByteBuf s : slices) {
                int sliceLength = s.readableBytes();
                ByteBuffer nioSlice = s.nioBuffer();
                int n = target.write(nioSlice);
                transferred += n;
                if (n < sliceLength) break;
            }
            return transferred;
        } finally {
            for (ByteBuf s : slices) s.release();
        }
    }

    @Override
    public CompletableFuture<Long> write(AsyncFile file, ByteBuf data) {
        if (!file.canWrite()) {
            data.release();
            throw new IllegalArgumentException("operation requires write mode: " + file.getKey());
        }
        if (file.cacheClosed) {
            data.release();
            throw new IllegalStateException("file cache is closed: " + file.getKey());
        }
        if (file.atomicReplace && data.readableBytes() == 0) {
            data.release();
            throw new IllegalArgumentException("atomic replace requires non-empty data: " + file.getKey());
        }
        return writeInternal(file, data,
                () -> initCacheAndAppend(file, data),
                writeBuf -> executeWithEioRetry(file, () -> delegate.writeSync(file, writeBuf)),
                () -> executeWithEioRetry(file, () -> {
                    delegate.fsyncSync(file);
                    return null;
                }));
    }

    private CompletableFuture<Long> writeInternal(AbstractStorageFile file, ByteBuf data,
            Runnable initCacheAndAppend,
            java.util.function.Function<ByteBuf, Long> fsWrite,
            Runnable fsFsync) {
        FileCacheEntry entry = file.getCacheEntry();
        final long writeSize = data.readableBytes();
        final boolean useCacheRequested = useCache(file);
        final String id = file.getKey();
        // First cache build (sizeSync/preload/atomic) must see settled FS; skip wait if cache already live.
        if (useCacheRequested && entry != null && !entry.isInitialized()) {
            try {
                if (hasInFlightIo(id)) {
                    awaitInFlightIo(id);
                }
            } catch (Exception e) {
                data.release();
                throw e;
            }
        }

        boolean useCacheSnapshot = useCacheRequested;
        if (useCacheRequested) {
            try {
                initCacheAndAppend.run();
            } catch (CacheFileTooLargeException e) {
                useCacheSnapshot = false;
            } catch (Exception e) {
                data.release();
                throw e;
            }
        }
        try {
            if (hasInFlightIo(id)) {
                if (!useCacheSnapshot || file.atomicReplace) {
                    awaitInFlightIo(id);
                } else {
                    data.release();
                    return CompletableFuture.completedFuture(writeSize);
                }
            }
        } catch (Exception e) {
            data.release();
            throw e;
        }

        if (!useCacheSnapshot && entry != null && entry.isInitialized()) {
            try {
                flushPendingWriteAndAwait(file, fsWrite, fsFsync, true);
            } catch (RuntimeException e) {
                data.release();
                throw e;
            }
            entry.reset();
        }

        final boolean useCache = useCacheSnapshot;
        final ByteBuf writeBuf;
        final long atomicIoGen;
        if (!useCacheSnapshot) {
            writeBuf = data;
            atomicIoGen = 0;
        } else if (file.atomicReplace) {
            writeBuf = data;
            atomicIoGen = entry.cacheGen;
        } else {
            long pending = entry.cacheEndOffset - entry.writtenToFsOffset;
            if (pending == writeSize) {
                writeBuf = data;
            } else {
                writeBuf = buildWriteBufAfterInFlight(entry);
                data.release();
            }
            atomicIoGen = 0;
        }
        if (!writeBuf.isReadable()) {
            writeBuf.release();
            return CompletableFuture.completedFuture(writeSize);
        }
        CompletableFuture<Long> ioFuture = StorageUtil.supply(ioExecutor, () -> {
            if (file.cacheClosed) {
                writeBuf.release();
                throw new IllegalStateException("file cache is closed: " + file.getKey());
            }
            long written = fsWrite.apply(writeBuf);
            if (useCache) {
                synchronized (entry) {
                    if (file.atomicReplace) {
                        if (atomicIoGen > entry.writtenGen) {
                            entry.writtenGen = atomicIoGen;
                        }
                        entry.writtenToFsOffset = written;
                    } else {
                        entry.writtenToFsOffset += written;
                    }
                    entry.pendingFsyncBytes = file.pendingFsyncBytes;
                }
            }
            return writeSize;
        }, writeBuf);
        registerInFlight(id, ioFuture);
        if (!useCacheSnapshot) {
            return ioFuture;
        }
        return CompletableFuture.completedFuture(writeSize);
    }

    private boolean useCache(AbstractStorageFile file) {
        if (backingFsMode == BackingFsMode.NO_CACHE || file.cacheMode == CacheMode.NO_CACHE) {
            return false;
        }
        FileCacheEntry entry = file.getCacheEntry();
        return !entry.largeFile;
    }

    private void initCacheAndAppend(AsyncFile file, ByteBuf data) {
        FileCacheEntry entry = file.getCacheEntry();

        ByteBuf view = data.duplicate();
        if (file.atomicReplace) {
            replaceAtomicCache(file, entry, view);
            return;
        }
        if (file.cacheMode == CacheMode.FULL_CACHE) {
            initFullCacheAndAppend(file, entry, view);
            return;
        }
        initTailCacheAndAppend(file, entry, view,
                () -> executeWithEioRetry(file, () -> delegate.sizeSync(file)));
    }

    private void initCacheAndAppend(AsyncSegmentFile file, ByteBuf data) {
        FileCacheEntry entry = file.getCacheEntry();
        initTailCacheAndAppend(file, entry, data.duplicate(), () -> segmentExclusiveEndOffset(file));
    }

    private void initFullCacheAndAppend(AsyncFile file, FileCacheEntry entry, ByteBuf data) {
        if (!entry.isInitialized()) {
            loadFullFileCache(file, data.readableBytes(), true);
        } else {
            // reserve and allocate chunks for the new data
            long startOffset = entry.cacheEndOffset;
            long endOffset = startOffset + data.readableBytes();
            long first = startOffset / chunkSize;
            long last = (endOffset - 1) / chunkSize;
            long newFirst = entry.chunks.containsKey(first) ? first + 1 : first;
            int newChunkCount = (int) (last - newFirst + 1);
            long newBytes = newChunkCount * chunkSize;
            boolean dirty = entry.cacheEndOffset > entry.writtenToFsOffset - entry.pendingFsyncBytes;
            if (!dirty && entry.bodySizeBytes + newBytes > maxCacheSizePerFileBytes) {
                entry.largeFile = true;
                throw new CacheFileTooLargeException(file.getKey(), entry.bodySizeBytes + newBytes);
            }
            memoryTracker.reserve(newBytes, maxCacheSizeBytes, ioWaitTimeoutMs);
            ByteBuf[] bufs = new ByteBuf[newChunkCount];
            try {
                for (int j = 0; j < newChunkCount; j++) {
                    bufs[j] = StorageAllocator.ALLOC.directBuffer((int) chunkSize);
                }
            } catch (Throwable t) {
                for (ByteBuf b : bufs) { if (b != null) b.release(); }
                memoryTracker.release(newBytes);
                throw new CacheMemoryReserveException(newBytes, t);
            }
            synchronized (entry) {
                for (int j = 0; j < newChunkCount; j++) {
                    entry.putChunk(newFirst + j, new CacheChunk(bufs[j]));
                }
            }
        }
        synchronized (entry) {
            entry.appendToChunkedCache(data, 0, chunkSize);
        }
    }

    private void initTailCacheAndAppend(AbstractStorageFile file, FileCacheEntry entry, ByteBuf data,
            java.util.function.Supplier<Long> endOffsetSupplier) {
        if (!entry.isInitialized()) {
            initTailCacheSync(file, endOffsetSupplier);
        }
        final long newFirst;
        final int newChunkCount;
        final long nowNanos = System.nanoTime();
        synchronized (entry) {
            long startOffset = entry.cacheEndOffset;
            long endOffset = startOffset + data.readableBytes();
            long first = startOffset / chunkSize;
            long last = (endOffset - 1) / chunkSize;
            newFirst = entry.chunks.containsKey(first) ? first + 1 : first;
            newChunkCount = (int) (last - newFirst + 1);
            if (newChunkCount > 0) {
                evictTailBeforeAppend(file.getKey(), entry, newChunkCount, nowNanos);
            }
        }
        long newBytes = newChunkCount * chunkSize;
        memoryTracker.reserve(newBytes, maxCacheSizeBytes, ioWaitTimeoutMs);
        ByteBuf[] bufs = new ByteBuf[newChunkCount];
        try {
            for (int j = 0; j < newChunkCount; j++) {
                bufs[j] = StorageAllocator.ALLOC.directBuffer((int) chunkSize);
            }
        } catch (Throwable t) {
            for (ByteBuf b : bufs) { if (b != null) b.release(); }
            memoryTracker.release(newBytes);
            throw new CacheMemoryReserveException(chunkSize, t);
        }
        synchronized (entry) {
            for (int j = 0; j < newChunkCount; j++) {
                entry.putChunk(newFirst + j, new CacheChunk(bufs[j]));
            }
            entry.appendToChunkedCache(data, nowNanos, chunkSize);
        }
    }

    // Must be called under synchronized(entry).
    private void replaceAtomicCache(AsyncFile file, FileCacheEntry entry, ByteBuf data) {
        int length = data.readableBytes();
        if (length > maxCacheSizePerFileBytes) {
            entry.largeFile = true;
            throw new CacheFileTooLargeException(file.getKey(), length);
        }
        CacheChunk old = entry.chunks.get(0L);
        long oldBytes = old == null ? 0 : old.buffer.capacity();
        final long delta = length - oldBytes;
        if (delta > 0) {
            memoryTracker.reserve(delta, maxCacheSizeBytes, ioWaitTimeoutMs);
        }
        ByteBuf newBuffer;
        try {
            newBuffer = StorageAllocator.ALLOC.directBuffer(length);
        } catch (Throwable t) {
            if (delta > 0) memoryTracker.release(delta);
            throw new CacheMemoryReserveException(length, t);
        }
        newBuffer.writeBytes(data, length);
        synchronized (entry) {
            entry.setAtomicChunk(new CacheChunk(newBuffer), 0);
        }
    }

    private void evictTailBeforeAppend(String fileKey, FileCacheEntry entry, int newChunks, long nowNanos) {
        int existingChunks = entry.chunks.size();
        int maxEvictable = Math.max(0, existingChunks - minRetainChunks);
        if (maxEvictable <= 0) return;
        Pair<Integer, Long> decision = decideEvictionPolicy(fileKey, maxEvictable, newChunks);
        long minEvict = decision.getKey();
        long durableFsOffset = Math.max(0, entry.writtenToFsOffset - entry.pendingFsyncBytes);
        long expireBeforeNanos = nowNanos - TimeUnit.MILLISECONDS.toNanos(decision.getValue());
        int evicted = 0;
        long index = entry.cacheStartOffset / chunkSize;
        long chunkEnd = (index + 1) * chunkSize;
        boolean durableLimit = false;
        while (evicted < maxEvictable) {
            CacheChunk chunk = entry.chunks.get(index);
            if (chunkEnd > durableFsOffset) { durableLimit = true; break; }
            if (chunk.lastAppendNanos > expireBeforeNanos) break;
            evicted++;
            index++;
            chunkEnd += chunkSize;
        }
        if (!durableLimit) {
            while (evicted < minEvict) {
                if (chunkEnd > durableFsOffset) break;
                evicted++;
                index++;
                chunkEnd += chunkSize;
            }
        }
        if (evicted > 0) {
            entry.dropCacheBefore(index * chunkSize, chunkSize);
        }
    }

    private Pair<Integer, Long> decideEvictionPolicy(String fileKey, int maxEvictable, int newChunks) {
        double low = lowWatermarkRatio;
        double high = highWatermarkRatio;
        if (low >= high) {
            return Pair.of(0, expectedMinRetentionMs);
        }
        double ratio = (double) memoryTracker.committedBytes() / maxCacheSizeBytes;
        if (ratio < low) {
            return Pair.of(0, expectedMinRetentionMs);
        }
        final long retentionMs;
        final int minEvict;
        if (ratio < high) {
            double pressureFactor = (ratio - low) / (high - low);
            Double evictRatio = fileEvictRatios.get(fileKey);
            if (evictRatio == null) {
                return Pair.of(0, expectedMinRetentionMs);
            }
            retentionMs = (long) (expectedMinRetentionMs * (1 - 0.5 * pressureFactor));
            minEvict = (int) Math.round(maxEvictable * evictRatio * (1 + pressureFactor));
        } else {
            retentionMs = expectedMinRetentionMs / 2;
            minEvict = (int) Math.round(maxEvictable * maxEvictRatioPerWrite);
        }
        return Pair.of(Math.min(maxEvictable, minEvict + newChunks), retentionMs);
    }

    // Empty buf with ioGen == 0: nothing to flush.
    // ioGen == 0 with data: no cache write.
    // ioGen > 0 with data: after FS write, update writtenGen to ioGen.
    private Pair<Long, ByteBuf> getPendingAtomicWriteBufAfterInFlight(FileCacheEntry entry) {
        if (!entry.isInitialized()) {
            return Pair.of(0L, Unpooled.buffer(0));
        }
        if (entry.writtenGen > entry.cacheGen) {
            logger.warn("atomic cache generation {} is behind written generation {}, advancing it",
                    entry.cacheGen, entry.writtenGen);
            entry.cacheGen = entry.writtenGen + 1;
        }
        if (entry.cacheGen == entry.writtenGen) {
            return Pair.of(0L, Unpooled.buffer(0));
        }
        if (entry.cacheEndOffset <= entry.cacheStartOffset) {
            logger.error("atomic dirty but empty cache range for gen {} (writtenGen {}), fixing writtenGen",
                    entry.cacheGen, entry.writtenGen);
            entry.writtenGen = entry.cacheGen;
            return Pair.of(0L, Unpooled.buffer(0));
        }
        long ioGen = entry.cacheGen;
        java.util.List<ByteBuf> slices = collectCacheSlices(
                entry, entry.cacheStartOffset, entry.cacheEndOffset, true, true);
        return Pair.of(ioGen, slices.get(0));
    }

    // Must be called only when there is no in-flight IO for this file.
    private ByteBuf buildWriteBufAfterInFlight(FileCacheEntry entry) {
        long pending = Math.max(0, entry.cacheEndOffset - entry.writtenToFsOffset);
        if (pending < writeBatchBytes) {
            return Unpooled.buffer(0);
        }
        return buildWriteBufFromCache(entry, maxWriteChunkThreshold * chunkSize);
    }

    private ByteBuf buildWriteBufFromCache(FileCacheEntry entry, long maxBytes) {
        long pendingBytes = Math.max(0, entry.cacheEndOffset - entry.writtenToFsOffset);
        if (pendingBytes <= 0) {
            return Unpooled.buffer(0);
        }
        boolean overflow = pendingBytes > maxBytes;
        long collectEnd = overflow
                ? entry.writtenToFsOffset + Math.min(pendingBytes, maxBytes)
                : entry.cacheEndOffset;
        java.util.List<ByteBuf> pending = collectChunkSlices(entry, entry.writtenToFsOffset, collectEnd, true);
        CompositeByteBuf composed = StorageAllocator.ALLOC.compositeDirectBuffer();
        for (ByteBuf s : pending) {
            composed.addComponent(true, s);
        }
        return composed;
    }

    @Override
    public CompletableFuture<Void> delete(String path) {
        return delegate.delete(path);
    }


    @Override
    public CompletableFuture<Void> delete(AsyncFile file) {
        StorageUtil.requireWriteMode(file);
        StorageUtil.requireCacheOpen(file);
        final String id = file.getKey();
        awaitInFlightIo(id);
        FileCacheEntry entry = file.getCacheEntry();
        if (entry != null) {
            synchronized (entry) {
                if (entry.isInitialized()) {
                    entry.clear();
                }
            }
        }
        return StorageUtil.run(ioExecutor, () -> {
            StorageUtil.requireCacheOpen(file);
            delegate.deleteSync(file.path);
        });
    }

    @Override
    public CompletableFuture<Boolean> exists(String path) {
        return delegate.exists(path);
    }

    @Override
    public CompletableFuture<Long> size(AsyncFile file) {
        StorageUtil.requireCacheOpen(file);
        if (file.cacheMode != CacheMode.NO_CACHE) {
            FileCacheEntry entry = file.getCacheEntry();
            synchronized (entry) {
                if (entry.isInitialized()) {
                    return CompletableFuture.completedFuture(
                            Math.max(entry.writtenToFsOffset, entry.cacheEndOffset));
                }
            }
        }
        return StorageUtil.supply(ioExecutor, () -> {
            StorageUtil.requireCacheOpen(file);
            return executeWithEioRetry(file, () -> delegate.sizeSync(file));
        });
    }


    @Override
    public CompletableFuture<Boolean> mkdir(String path, boolean recursive) {
        return delegate.mkdir(path, recursive);
    }

    @Override
    public CompletableFuture<Boolean> rmdir(String path, boolean recursive) {
        return delegate.rmdir(path, recursive);
    }

    @Override
    public CompletableFuture<Void> truncate(AsyncFile file, long size) {
        StorageUtil.requireWriteMode(file);
        StorageUtil.requireCacheOpen(file);
        final String id = file.getKey();
        awaitInFlightIo(id);
        FileCacheEntry entry = file.getCacheEntry();
        if (entry != null) {
            synchronized (entry) {
                if (entry.isInitialized() && size < entry.cacheEndOffset) {
                    if (file.atomicReplace) {
                        ByteBuf newChunk;
                        try {
                            newChunk = StorageAllocator.ALLOC.directBuffer((int) size);
                        } catch (Throwable e) {
                            throw new CacheMemoryReserveException(size, e);
                        }
                        CacheChunk oldChunk = entry.chunks.get(0L);
                        newChunk.writeBytes(oldChunk.buffer, 0, (int) size);
                        entry.setAtomicChunk(new CacheChunk(newChunk), Math.min(size, entry.writtenToFsOffset));
                    } else {
                        entry.truncateTo(size, chunkSize);
                    }
                }
            }
        }
        CompletableFuture<Void> ioFuture = StorageUtil.run(ioExecutor, () -> {
            StorageUtil.requireCacheOpen(file);
            executeWithEioRetry(file, () -> {
                delegate.truncateSync(file, size);
                return null;
            });
            if (entry != null) {
                entry.pendingFsyncBytes = file.pendingFsyncBytes;
                entry.largeFile = false;
            }
        });
        registerInFlight(id, ioFuture);
        return ioFuture;
    }


    @Override
    public CompletableFuture<Void> close(AsyncFile file) {
        if (!file.canCloseByUser) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("close is not allowed for: " + file.getKey()));
        }
        return closeInternal(file,
                () -> delegate.closeSync(file),
                f -> asyncFileFlushPendingWriteAndAwait(f, false));
    }

    private <T extends AbstractStorageFile> CompletableFuture<Void> closeInternal(T file,
            Runnable fsClose, java.util.function.Consumer<T> flush) {
        if (file.cacheClosed) {
            return CompletableFuture.completedFuture(null);
        }
        final String id = file.getKey();
        awaitInFlightIo(id);
        flush.accept(file);
        file.cacheClosed = true;
        return StorageUtil.run(ioExecutor, () -> {
            try {
                fsClose.run();
            } finally {
                file.onCacheClose.run();
            }
        });
    }


    @Override
    public CompletableFuture<Void> fsync(AsyncFile file) {
        return fsyncInternal(file,
                () -> asyncFileFlushPendingWriteAndAwait(file, false),
                () -> executeWithEioRetry(file, () -> {
                    delegate.fsyncSync(file);
                    return null;
                }));
    }

    private CompletableFuture<Void> fsyncInternal(AbstractStorageFile file, Runnable flushPending, Runnable fsFsync) {
        StorageUtil.requireWriteMode(file);
        StorageUtil.requireCacheOpen(file);
        final String id = file.getKey();
        final FileCacheEntry entry = file.getCacheEntry();
        awaitInFlightIo(id);
        flushPending.run();
        CompletableFuture<Void> ioFuture = StorageUtil.run(ioExecutor, () -> {
            StorageUtil.requireCacheOpen(file);
            fsFsync.run();
            if (entry != null) {
                entry.pendingFsyncBytes = file.pendingFsyncBytes;
            }
        });
        registerInFlight(id, ioFuture);
        return ioFuture;
    }


    @Override
    public CompletableFuture<List<String>> list(String path) {
        return delegate.list(path);
    }

    @Override
    public CompletableFuture<Long> transferTo(AsyncFile file, long position, long count, WritableByteChannel target) {
        StorageUtil.requireCacheOpen(file);
        return transferToInternal(file, position, count, target,
                () -> executeWithEioRetry(file,
                        () -> delegate.transferToSync(file, position, count, target)));
    }

    private CompletableFuture<Long> transferToInternal(AbstractStorageFile file, long offset, long count,
            WritableByteChannel target, java.util.function.Supplier<Long> fsTransfer) {
        StorageUtil.requireCacheOpen(file);
        FileCacheEntry entry = file.getCacheEntry();
        if (preferDirectRead(file, entry, offset, transferPreferCache)) {
            return StorageUtil.supply(ioExecutor, () -> {
                StorageUtil.requireCacheOpen(file);
                return fsTransfer.get();
            });
        }
        try {
            return CompletableFuture.completedFuture(
                    transferToByCache(entry, offset, count, target, file.atomicReplace));
        } catch (IOException e) {
            return CompletableFuture.failedFuture(new SocketErrorException(e));
        }
    }


    // ---- AsyncSegmentFile ----

    @Override
    public CompletableFuture<AsyncSegmentFile> open(String path, String prefix, List<String> indexPrefixes, boolean write, String tenant) {
        return StorageUtil.supply(ioExecutor, () -> openSegmentSync(path, prefix, indexPrefixes, write, tenant, null));
    }

    public CompletableFuture<AsyncSegmentFile> open(String path, String prefix, List<String> indexPrefixes, boolean write, String tenant, CacheMode cacheMode) {
        return StorageUtil.supply(ioExecutor, () -> openSegmentSync(path, prefix, indexPrefixes, write, tenant, cacheMode));
    }


    private AsyncSegmentFile openSegmentSync(String path, String prefix, List<String> indexPrefixes, boolean write, String tenant, CacheMode cacheModeOverride) {
        CacheMode cacheMode = resolveSegmentCacheMode(cacheModeOverride);
        String key = StorageUtil.segmentKey(path, prefix);
        AsyncSegmentFile file = delegate.openSync(path, prefix, indexPrefixes, write, tenant);
        file.cacheMode = cacheMode;
        if (cacheMode != CacheMode.NO_CACHE) {
            file.setIndexCacheInitializer(this::initFileCache);
            SegmentFileCacheEntry entry;
            boolean first;
            try {
                synchronized (lockFor(key)) {
                    entry = segmentCacheEntries.computeIfAbsent(
                            key, k -> new SegmentFileCacheEntry(memoryTracker));
                    first = entry.retainEntry(write);
                }
            } catch (Throwable t) {
                logger.error("acquire segment cache entry failed for {}, closing file", file.getKey(), t);
                cleanupOpenFailedSegment(file);
                throw t;
            }
            file.setCacheEntry(entry);
            file.onCacheClose = () -> {
                SegmentFileCacheEntry shared = file.getCacheEntry();
                if (shared == null) {
                    return;
                }
                synchronized (lockFor(key)) {
                    if (shared.releaseEntry(write)) {
                        segmentCacheEntries.remove(key, shared);
                    }
                }
            };
            initSegmentCache(file, first);
        }
        return file;
    }

    // Exclusive logical end offset of the segment file (firstOffset + size); 0 if empty.
    private long segmentExclusiveEndOffset(AsyncSegmentFile file) {
        List<Long> offsets = list(file);
        return offsets.isEmpty() ? 0L : offsets.get(0) + delegate.sizeSync(file);
    }

    @Override
    public CompletableFuture<Void> position(AsyncSegmentFile file, long offset) {
        if (!file.canRead()) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("position() is not supported in write mode"));
        }
        StorageUtil.requireCacheOpen(file);
        return StorageUtil.run(ioExecutor, () -> {
            StorageUtil.requireCacheOpen(file);
            executeWithEioRetry(file, () -> {
                delegate.positionSync(file, offset);
                return null;
            });
        });
    }


    @Override
    public CompletableFuture<ByteBuf> read(AsyncSegmentFile file, long length) {
        long readOffset = file.position;
        return readInternal(file, length, 0, true,
                () -> executeWithEioRetry(file, () -> delegate.readSync(file, length, readOffset)));
    }


    @Override
    public CompletableFuture<ByteBuf> read(AsyncSegmentFile file, long length, long offset) {
        return readInternal(file, length, offset, false,
                () -> executeWithEioRetry(file, () -> delegate.readSync(file, length, offset)));
    }


    @Override
    public CompletableFuture<Long> write(AsyncSegmentFile file, ByteBuf data) {
        if (!file.canWrite()) {
            data.release();
            throw new IllegalArgumentException("operation requires write mode: " + file.getKey());
        }
        if (file.cacheClosed) {
            data.release();
            throw new IllegalStateException("file cache is closed: " + file.getKey());
        }
        if (delegate.list(file).isEmpty()) {
            try {
                final String id = file.getKey();
                awaitInFlightIo(id);
                if (delegate.list(file).isEmpty()) {
                    CompletableFuture<Map<String, AsyncFile>> ioFuture = StorageUtil.supply(ioExecutor, () -> {
                        StorageUtil.requireCacheOpen(file);
                        return executeWithEioRetry(file, () -> delegate.rollSync(file));
                    });
                    registerInFlight(id, ioFuture);
                    awaitInFlightIo(id);
                }
            } catch (Throwable t) {
                data.release();
                throw t;
            }
        }
        return writeInternal(file, data,
                () -> initCacheAndAppend(file, data),
                writeBuf -> executeWithEioRetry(file, () -> delegate.writeSync(file, writeBuf)),
                () -> executeWithEioRetry(file, () -> {
                    delegate.fsyncSync(file);
                    return null;
                }));
    }


    @Override
    public CompletableFuture<Map<String, AsyncFile>> roll(AsyncSegmentFile file) {
        StorageUtil.requireWriteMode(file);
        StorageUtil.requireCacheOpen(file);
        final String id = file.getKey();
        awaitInFlightIo(id);
        segmentFlushPendingWriteAndAwait(file, false);
        CompletableFuture<Map<String, AsyncFile>> ioFuture = StorageUtil.supply(ioExecutor, () -> {
            StorageUtil.requireCacheOpen(file);
            return executeWithEioRetry(file, () -> delegate.rollSync(file));
        });
        registerInFlight(id, ioFuture);
        return ioFuture;
    }


    @Override
    public List<Long> list(AsyncSegmentFile file) {
        return delegate.list(file);
    }

    @Override
    public long getCurrentSegmentStartOffset(AsyncSegmentFile file) {
        return delegate.getCurrentSegmentStartOffset(file);
    }

    @Override
    public CompletableFuture<Map<String, AsyncFile>> getCurrentIndexFiles(AsyncSegmentFile file, List<String> indexPrefixes) {
        StorageUtil.requireCacheOpen(file);
        return StorageUtil.supply(ioExecutor, () -> {
            StorageUtil.requireCacheOpen(file);
            return executeWithEioRetry(file, () -> delegate.getCurrentIndexFilesSync(file, indexPrefixes));
        });
    }


    @Override
    public CompletableFuture<Map<String, AsyncFile>> getCurrentIndexFiles(AsyncSegmentFile file) {
        return getCurrentIndexFiles(file, file.indexPrefixes);
    }


    @Override
    public CompletableFuture<Long> size(AsyncSegmentFile file) {
        StorageUtil.requireCacheOpen(file);
        if (file.cacheMode != CacheMode.NO_CACHE) {
            SegmentFileCacheEntry entry = file.getCacheEntry();
            List<Long> offsets = list(file);
            if (offsets.isEmpty()) {
                return CompletableFuture.completedFuture(0L);
            }
            long firstOffset = offsets.get(0);
            synchronized (entry) {
                if (entry.isInitialized()) {
                    long end = Math.max(entry.writtenToFsOffset, entry.cacheEndOffset);
                    return CompletableFuture.completedFuture(Math.max(0L, end - firstOffset));
                }
            }
        }
        return StorageUtil.supply(ioExecutor, () -> {
            StorageUtil.requireCacheOpen(file);
            return executeWithEioRetry(file, () -> delegate.sizeSync(file));
        });
    }


    @Override
    public CompletableFuture<Long> sizeOfSegment(AsyncSegmentFile file, long startOffset) {
        StorageUtil.requireCacheOpen(file);
        if (file.cacheMode != CacheMode.NO_CACHE) {
            List<Long> offsets = list(file);
            if (offsets.isEmpty()) {
                return CompletableFuture.completedFuture(0L);
            }
            int idx = Collections.binarySearch(offsets, startOffset);
            if (idx < 0) {
                return CompletableFuture.completedFuture(0L);
            }
            if (idx + 1 < offsets.size()) {
                return CompletableFuture.completedFuture(offsets.get(idx + 1) - startOffset);
            }
            SegmentFileCacheEntry entry = file.getCacheEntry();
            synchronized (entry) {
                if (entry.isInitialized()) {
                    long end = Math.max(entry.writtenToFsOffset, entry.cacheEndOffset);
                    return CompletableFuture.completedFuture(Math.max(0L, end - startOffset));
                }
            }
        }
        return StorageUtil.supply(ioExecutor, () -> {
            StorageUtil.requireCacheOpen(file);
            return executeWithEioRetry(file, () -> delegate.sizeOfSegmentSync(file, startOffset));
        });
    }


    @Override
    public CompletableFuture<Long> lastModified(AsyncSegmentFile file) {
        return delegate.lastModified(file);
    }

    @Override
    public CompletableFuture<Long> lastModifiedOfSegment(AsyncSegmentFile file, long startOffset) {
        return delegate.lastModifiedOfSegment(file, startOffset);
    }

    @Override
    public CompletableFuture<Void> deleteSegments(AsyncSegmentFile file, List<Long> startOffsets) {
        StorageUtil.requireWriteMode(file);
        StorageUtil.requireCacheOpen(file);
        if (startOffsets.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        List<Long> offsets = list(file);
        int drop = startOffsets.size();
        if (drop >= offsets.size()) {
            throw new IllegalArgumentException("deleteSegments cannot delete the last segment");
        }
        for (int i = 0; i < drop; i++) {
            if (!startOffsets.get(i).equals(offsets.get(i))) {
                throw new IllegalArgumentException(
                        "deleteSegments requires deleting segments in order from the first: expected "
                                + offsets.get(i) + ", got " + startOffsets.get(i));
            }
        }
        long newFirstOffset = offsets.get(drop);
        if (file.cacheMode != CacheMode.NO_CACHE) {
            SegmentFileCacheEntry entry = file.getCacheEntry();
            synchronized (entry) {
                if (entry.isInitialized()) {
                    long cacheStart = entry.cacheStartOffset;
                    if (cacheStart < newFirstOffset) {
                        entry.dropCacheBefore(newFirstOffset, chunkSize);
                    }
                }
            }
        }
        final String id = file.getKey();
        awaitInFlightIo(id);
        CompletableFuture<Void> ioFuture = StorageUtil.run(ioExecutor, () -> {
            StorageUtil.requireCacheOpen(file);
            executeWithEioRetry(file, () -> {
                delegate.deleteSegmentsSync(file, startOffsets);
                return null;
            });
        });
        registerInFlight(id, ioFuture);
        return ioFuture;
    }

    @Override
    public CompletableFuture<Void> delete(AsyncSegmentFile file) {
        StorageUtil.requireWriteMode(file);
        StorageUtil.requireCacheOpen(file);
        final String id = file.getKey();
        awaitInFlightIo(id);
        if (file.cacheMode != CacheMode.NO_CACHE) {
            SegmentFileCacheEntry entry = file.getCacheEntry();
            synchronized (entry) {
                if (entry.isInitialized()) {
                    entry.clear();
                }
            }
        }
        CompletableFuture<Void> ioFuture = delegate.delete(file);
        return ioFuture;
    }

    @Override
    public CompletableFuture<Map<String, AsyncFile>> truncate(AsyncSegmentFile file, long offset) {
        StorageUtil.requireWriteMode(file);
        StorageUtil.requireCacheOpen(file);
        if (file.cacheMode != CacheMode.NO_CACHE) {
            SegmentFileCacheEntry entry = file.getCacheEntry();
            synchronized (entry) {
                if (entry.isInitialized()) {
                    List<Long> offsets = list(file);
                    if (!offsets.isEmpty()) {
                        entry.truncateTo(offset, chunkSize, offsets.get(0));
                    }
                }
            }
        }
        final String id = file.getKey();
        awaitInFlightIo(id);
        CompletableFuture<Map<String, AsyncFile>> ioFuture = StorageUtil.supply(ioExecutor, () -> {
            StorageUtil.requireCacheOpen(file);
            Map<String, AsyncFile> result = executeWithEioRetry(file, () -> delegate.truncateSync(file, offset));
            if (file.getCacheEntry() != null) {
                file.getCacheEntry().pendingFsyncBytes = file.pendingFsyncBytes;
            }
            return result;
        });
        registerInFlight(id, ioFuture);
        return ioFuture;
    }


    @Override
    public CompletableFuture<Void> close(AsyncSegmentFile file) {
        return closeInternal(file, () -> delegate.closeSync(file), f -> segmentFlushPendingWriteAndAwait(f, false));
    }

    @Override
    public CompletableFuture<Void> fsync(AsyncSegmentFile file) {
        return fsyncInternal(file,
                () -> flushPendingWriteAndAwait(file,
                        writeBuf -> executeWithEioRetry(file,
                                () -> delegate.writeSync(file, writeBuf)),
                        () -> executeWithEioRetry(file, () -> {
                            delegate.fsyncSync(file);
                            return null;
                        }),
                        false),
                () -> executeWithEioRetry(file, () -> {
                    delegate.fsyncSync(file);
                    return null;
                }));
    }

    @Override
    public CompletableFuture<Long> transferTo(AsyncSegmentFile file, long offset, long count, WritableByteChannel target) {
        return transferToInternal(file, offset, count, target,
                () -> executeWithEioRetry(file,
                        () -> delegate.transferToSync(file, offset, count, target)));
    }

}
