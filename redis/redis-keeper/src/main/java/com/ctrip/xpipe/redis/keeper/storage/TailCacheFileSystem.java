package com.ctrip.xpipe.redis.keeper.storage;

import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
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
    private volatile long restoreWaitTimeoutMs;
    private volatile long writeBatchBytes;
    private volatile int maxWriteChunkThreshold;
    private volatile int eioRetryMaxAttempts;
    private final ExecutorService ioExecutor;
    private final CacheMemoryTracker memoryTracker = new CacheMemoryTracker();
    private final ScheduledExecutorService evictExecutor;
    private final ExecutorService closeExecutor;
    private final AtomicBoolean shuttingDown = new AtomicBoolean();
    private volatile Map<String, Double> fileEvictRatios = Collections.emptyMap();

    private final ConcurrentHashMap<String, FileCacheEntry> fileCacheEntries = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, SegmentFileCacheEntry> segmentCacheEntries = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CompletableFuture<?>> inFlightIo = new ConcurrentHashMap<>();

    private static final int READER_ID_STRIPES = 32;
    private final java.util.concurrent.atomic.AtomicLong[] readerIdCounters =
            new java.util.concurrent.atomic.AtomicLong[READER_ID_STRIPES];

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
        this.restoreWaitTimeoutMs = config.getRestoreWaitTimeoutMs();
        this.writeBatchBytes = config.getWriteBatchBytes();
        this.maxWriteChunkThreshold = config.getMaxWriteChunkThreshold();
        this.eioRetryMaxAttempts = config.getEioRetryMaxAttempts();
        this.ioExecutor = ioExecutor;
        for (int i = 0; i < LOCK_STRIPES; i++) locks[i] = new Object();
        for (int i = 0; i < READER_ID_STRIPES; i++) readerIdCounters[i] = new java.util.concurrent.atomic.AtomicLong(0);
        this.evictExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "tail-cache-evict-scanner");
            thread.setDaemon(true);
            return thread;
        });
        // shall never reject or fd leak
        this.closeExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread thread = new Thread(r, "tail-cache-close");
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

    public long getRestoreWaitTimeoutMs() {
        return restoreWaitTimeoutMs;
    }

    public void setRestoreWaitTimeoutMs(long restoreWaitTimeoutMs) {
        TailCacheFileSystemConfig.validateRestoreWaitTimeoutMs(restoreWaitTimeoutMs);
        this.restoreWaitTimeoutMs = restoreWaitTimeoutMs;
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
        closeExecutor.shutdown();
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
                // Async scan must not evict dirty / undurable data.
                evictTailBeforeAppend(fileKey, entry, 0, nowNanos, false);
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

    private void awaitInFlightIo(String id, String path, boolean throwOnFailure) {
        CompletableFuture<?> future = inFlightIo.get(id);
        if (future == null) {
            return;
        }
        StorageUtil.awaitFuture(future, path, ioWaitTimeoutMs, throwOnFailure);
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
        file.throwIfNoSpace();
        FileCacheEntry entry = file.getCacheEntry();
        if (!entry.isInitialized() || entry.fsInconsistent) {
            return;
        }
        boolean cacheDirty = entry.isCacheDirty(file.atomicReplace);
        boolean fsyncDirty = entry.isFsyncDirty();
        if (!cacheDirty && !fsyncDirty) {
            return;
        }
        final String id = file.ioKey;
        final String path = file.path;
        final ByteBuf writeBuf;
        final long ioGen;
        if (file.atomicReplace) {
            Pair<Long, ByteBuf> atomic = entry.getPendingAtomicWriteBufAfterInFlight();
            writeBuf = atomic.getValue();
            ioGen = atomic.getKey();
        } else {
            writeBuf = entry.buildWriteBufFromCache(Long.MAX_VALUE, chunkSize);
            ioGen = 0;
        }
        final boolean hasWriteData = writeBuf.isReadable();
        if (!hasWriteData && !fsyncDirty) {
            writeBuf.release();
            return;
        }
        CompletableFuture<Long> flushFuture = StorageUtil.supply(ioExecutor, () -> {
            if (file.closed) {
                writeBuf.release();
                throw new IllegalStateException("file is closed: " + path);
            }
            long written = 0;
            if (hasWriteData) {
                written = fsWrite.apply(writeBuf);
            } else {
                writeBuf.release();
            }
            synchronized (entry) {
                if (!entry.fsInconsistent) {
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
            }
            fsFsync.run();
            synchronized (entry) {
                if (!entry.fsInconsistent) {
                    entry.pendingFsyncBytes = file.pendingFsyncBytes;
                }
            }
            return written;
        }, writeBuf);
        registerInFlight(id, flushFuture);
        try {
            StorageUtil.awaitFuture(flushFuture, path, ioWaitTimeoutMs, false);
        } catch (Exception e) {
            // do nothing will get again to get the exact exception.
        }
        Throwable flushError = null;
        boolean retryableFailure = false;
        boolean incomplete = false;
        try {
            flushFuture.get(1, TimeUnit.NANOSECONDS);
        } catch (ExecutionException e) {
            flushError = e.getCause();
            retryableFailure = flushError instanceof EIOException;
        } catch (TimeoutException e) {
            flushError = e;
            retryableFailure = true;
            incomplete = true;
        } catch (Exception e) {
            flushError = e;
        }

        // throw no space error to let the caller handle it even failIfStillDirty = false
        file.throwIfNoSpace();

        // throw even if failIfStillDirty = false as this method is also io task barrier.
        if (incomplete) {
            throw new OperationNotExecutedException(path, flushError);
        }

        boolean stillDirty = entry.isCacheDirty(file.atomicReplace)
                || (!file.atomicReplace && entry.isFsyncDirty());
        if (stillDirty) {
            if (retryableFailure) {
                throw new OperationNotExecutedException(path, flushError);
            }
            if (failIfStillDirty) {
                throw new IllegalStateException(
                        "unflushed cache remains after drain for " + path + "; refusing write to avoid corruption",
                        flushError);
            }
            long remainingBytes = Math.max(0, entry.cacheEndOffset - entry.writtenToFsOffset);
            if (flushError != null) {
                logger.error("data lost for {}, still has {} bytes not flushed after flush, cacheGen={}, writtenGen={}",
                        path, remainingBytes, entry.cacheGen, entry.writtenGen, flushError);
            } else {
                logger.error("data lost for {}, still has {} bytes not flushed after flush with non error, cacheGen={}, writtenGen={}",
                        path, remainingBytes, entry.cacheGen, entry.writtenGen);
            }
            return;
        }
        if (flushError != null) {
            logger.error("flush finished with error for {}, unexpected", path, flushError);
        }
    }

    // require file inflight io to be completed before calling this
    private void asyncFileFlushPendingWriteAndAwait(AsyncFile file, boolean failIfStillDirty) {
        flushPendingWriteAndAwait(file,
                writeBuf -> executeWithIoFailureHandling(file, () -> delegate.writeSync(file, writeBuf)),
                () -> executeWithIoFailureHandling(file, () -> {
                    delegate.fsyncSync(file);
                    return null;
                }),
                failIfStillDirty);
    }

    // require segment inflight io to be completed before calling this
    private void segmentFlushPendingWriteAndAwait(AsyncSegmentFile file, boolean failIfStillDirty) {
        flushPendingWriteAndAwait(file,
                writeBuf -> executeWithIoFailureHandling(file, () -> delegate.writeSync(file, writeBuf)),
                () -> executeWithIoFailureHandling(file, () -> {
                    delegate.fsyncSync(file);
                    return null;
                }),
                failIfStillDirty);
        for (AsyncIndexFile indexFile : file.currentIndexFiles.values()) {
            asyncFileFlushPendingWriteAndAwait(indexFile, failIfStillDirty);
        }
    }

    private <T> T executeWithIoFailureHandling(AbstractStorageFile file,
            java.util.function.Supplier<T> ioAction) {
        int maxAttempts = eioRetryMaxAttempts;
        try {
            return ioAction.get();
        } catch (RuntimeException e) {
            if (e instanceof StorageIOException && e.getCause() instanceof IOException
                    && StorageUtil.isNoSpace((IOException) e.getCause())) {
                file.markNoSpace((IOException) e.getCause());
                throw e;
            }
            if (!(e instanceof EIOException)) {
                throw e;
            }
            logger.warn("io action requires channel recovery for {}, retrying replacement up to {} times",
                    file.path, maxAttempts, e);
            for (int attempt = 1; attempt <= maxAttempts; attempt++) {
                if (file.closed) {
                    // Nothing to replace: openCurrentChannel would only discard the new channel.
                    break;
                }
                try {
                    long writtenToFsOffset = file.openCurrentChannel();
                    resetWrittenToFsOffsetIfNeeded(file, writtenToFsOffset);
                    break;
                } catch (Exception retryError) {
                    logger.error("channel replacement attempt {}/{} failed for {}",
                            attempt, maxAttempts, file.path, retryError);
                }
            }
            throw e;
        }
    }

    private void resetWrittenToFsOffsetIfNeeded(AbstractStorageFile file, long writtenToFsOffset) {
        if (!file.canWrite() || writtenToFsOffset < 0) {
            return;
        }
        FileCacheEntry entry = file.getCacheEntry();
        if (entry == null || entry.fsInconsistent) {
            return;
        }
        synchronized (entry) {
            if (entry.fsInconsistent) {
                return;
            }
            long oldWrittenToFsOffset = entry.writtenToFsOffset;
            if (oldWrittenToFsOffset != writtenToFsOffset) {
                logger.warn("align offset after channel replacement for {} from {} to {}",
                        file.path, oldWrittenToFsOffset, writtenToFsOffset);
                entry.writtenToFsOffset = writtenToFsOffset;
            }
        }
    }

    private void prepareFileSync(AbstractStorageFile file) {
        if (!file.needPrepare) {
            return;
        }
        try {
            delegate.mkdirSync(file.dirPath, true);
            file.openCurrentChannel();
        } catch (IOException e) {
            throw StorageUtil.wrapIOException(e);
        }
        file.needPrepare = false;
    }

    /**
     * Disk repair only; does not mutate cache entry. Returns writtenToFsOffset to apply,
     * or null if restore is not applicable / incomplete / failed.
     * Non-null means success.
     */
    private Long restoreFsConsistencySync(AsyncFile file) {
        FileCacheEntry entry = file.getCacheEntry();
        final long writtenToFsOffset;
        final long cacheStartOffset;
        synchronized (entry) {
            if (!entry.isInitialized()) {
                return null;
            }
            if (entry.writtenToFsOffset < entry.cacheStartOffset) {
                return null;
            }
            writtenToFsOffset = entry.writtenToFsOffset;
            cacheStartOffset = entry.cacheStartOffset;
        }
        try {
            long size = executeWithIoFailureHandling(file, () -> {
                delegate.truncateSync(file, writtenToFsOffset);
                return delegate.sizeSync(file);
            });
            if (size >= cacheStartOffset) {
                return size;
            }
            return null;
        } catch (Exception e) {
            logger.warn("failed to restore backing FS consistency for {}, leaving it inconsistent",
                    file.path, e);
            return null;
        }
    }

    /**
     * Disk repair only; does not mutate cache entry.
     * Delta: ([writtenToFsOffset, localReadableFromOffset], indexDeltas).
     * indexDeltas: startOffset → indexPrefix → writtenToFsOffset.
     * Non-null means success (caller clears fsInconsistent); null on incomplete/fail.
     */
    private Pair<List<Long>, Map<Long, Map<String, Long>>>
            restoreSegmentFsConsistencySync(AsyncSegmentFile file) {

        SegmentFileCacheEntry entry = file.getCacheEntry();

        try {
            SegmentDirState state = delegate.getSegmentDirState(file);
            if (state.isEmpty()) {
                delegate.deleteOrphanSegmentFilesSync(file);
                return segmentRestoreDelta(0L, 0L, Collections.emptyMap());
            }

            final long writtenToFsOffset;
            final long cacheStartOffset;
            final long cacheEndOffset;
            synchronized (entry) {
                if (!entry.isInitialized()) {
                    return null;
                }
                writtenToFsOffset = entry.writtenToFsOffset;
                cacheStartOffset = entry.cacheStartOffset;
                cacheEndOffset = entry.cacheEndOffset;
            }
            long lastStart = state.lastOffset;


            // Step 0: last segment's not-yet-written range must be in cache.
            long pendingStart = Math.max(writtenToFsOffset, lastStart);
            if (pendingStart < cacheStartOffset) {
                return null;
            }

            // Step 1
            delegate.deleteOrphanSegmentFilesSync(file);

            // Step 2: flush contiguous suffix from second-last back to written's segment.
            Pair<Long, Map<Long, Map<String, Long>>> prior =
                    restorePriorSegmentsFromCache(file, entry, state, lastStart,
                            writtenToFsOffset, cacheStartOffset);
            long localReadableFrom = prior.getKey();
            Map<Long, Map<String, Long>> indexDeltas = prior.getValue();

            // Step 3: align last only; recheck calibrated still in cache.
            Pair<Long, Map<Long, Map<String, Long>>> aligned =
                    alignLastSegmentForRestore(file, lastStart,
                            writtenToFsOffset, cacheStartOffset, cacheEndOffset);
            if (aligned == null) {
                return null;
            }
            for (Map.Entry<Long, Map<String, Long>> e : aligned.getValue().entrySet()) {
                for (Map.Entry<String, Long> ie : e.getValue().entrySet()) {
                    indexDeltas.computeIfAbsent(e.getKey(), k -> new HashMap<>())
                            .put(ie.getKey(), ie.getValue());
                }
            }

            // Step 4: return delta for user thread to apply.
            return segmentRestoreDelta(aligned.getKey(), localReadableFrom, indexDeltas);
        } catch (Exception e) {
            logger.warn("failed to restore segment backing FS consistency for {}, leaving it inconsistent",
                    file.path, e);
            return null;
        }
    }

    private static Pair<List<Long>, Map<Long, Map<String, Long>>> segmentRestoreDelta(
            long writtenToFsOffset, long localReadableFromOffset,
            Map<Long, Map<String, Long>> indexDeltas) {
        List<Long> offsets = new ArrayList<>(2);
        offsets.add(writtenToFsOffset);
        offsets.add(localReadableFromOffset);
        return Pair.of(offsets, indexDeltas);
    }

    // Returns (localReadableFromOffset, indexDeltas). localReadableFrom is 0 if contiguous
    // suffix reached written's segment.
    private Pair<Long, Map<Long, Map<String, Long>>> restorePriorSegmentsFromCache(
            AsyncSegmentFile file, SegmentFileCacheEntry entry, SegmentDirState state, long lastStart,
            long writtenToFsOffset, long cacheStartOffset) {
        Map<Long, Map<String, Long>> indexDeltas = new HashMap<>();
        int n = state.size();
        if (n < 2 || writtenToFsOffset >= lastStart) {
            return Pair.of(0L, indexDeltas);
        }

        long localReadableFrom = lastStart;
        for (int i = n - 2; i >= 0; i--) {
            long segStart = state.get(i);
            long segEnd = state.get(i + 1);
            final long start = segStart;
            final long end = segEnd;
            boolean rewritten = delegate.rewriteSegmentRangeSync(
                    file, start, writtenToFsOffset, logicalFrom -> {
                if (logicalFrom < cacheStartOffset) {
                    return null;
                }
                return entry.buildWriteBufFromCacheRange(logicalFrom, end, chunkSize);
            });
            if (!rewritten) {
                return Pair.of(localReadableFrom, indexDeltas);
            }
            // Segment repair succeeded; best-effort rewrite corresponding historical indexes.
            ConcurrentHashMap<String, FileCacheEntry> byPrefix = entry.indexFiles.get(start);
            if (byPrefix != null) {
                for (Map.Entry<String, FileCacheEntry> indexMapEntry : byPrefix.entrySet()) {
                    String indexPrefix = indexMapEntry.getKey();
                    FileCacheEntry indexEntry = indexMapEntry.getValue();
                    if (indexEntry == null || !indexEntry.isInitialized()
                            || !(indexEntry.fsInconsistent
                                    || indexEntry.isCacheDirty(false)
                                    || indexEntry.isFsyncDirty())) {
                        continue;
                    }
                    try {
                        final long written;
                        final long cacheEnd;
                        final long cacheStart;
                        synchronized (indexEntry) {
                            if (!indexEntry.isInitialized()) {
                                continue;
                            }
                            written = indexEntry.writtenToFsOffset;
                            cacheEnd = indexEntry.cacheEndOffset;
                            cacheStart = indexEntry.cacheStartOffset;
                        }
                        boolean indexRewritten = delegate.rewriteIndexRangeSync(
                                file, indexPrefix, start, written, from -> {
                            if (from < cacheStart) {
                                return null;
                            }
                            return indexEntry.buildWriteBufFromCacheRange(from, cacheEnd, chunkSize);
                        });
                        if (indexRewritten) {
                            indexDeltas.computeIfAbsent(start, k -> new HashMap<>())
                                    .put(indexPrefix, cacheEnd);
                        } else {
                            // TODO: decide how to handle failed index restore (invalidate disk
                            // index / keep flag / rely on caller rebuild from segment).
                            logger.warn("best-effort historical index rewrite failed for {}{} on {}",
                                    indexPrefix, start, file.path);
                        }
                    } catch (Exception ex) {
                        // TODO: same as above — failure policy undecided.
                        logger.warn("best-effort historical index rewrite threw for {}{} on {}",
                                indexPrefix, start, file.path, ex);
                    }
                }
            }
            localReadableFrom = segStart;
            if (writtenToFsOffset >= segStart) {
                return Pair.of(0L, indexDeltas);
            }
        }
        return Pair.of(0L, indexDeltas);
    }

    // Returns (calibrated writtenToFsOffset, last-index deltas), or null if recheck failed.
    private Pair<Long, Map<Long, Map<String, Long>>> alignLastSegmentForRestore(
            AsyncSegmentFile file, long lastStart,
            long writtenToFsOffset, long cacheStartOffset, long cacheEndOffset) {

        long diskEnd;
        try {
            diskEnd = file.openCurrentChannel();
        } catch (IOException e) {
            throw StorageUtil.wrapIOException(e);
        }

        final long calibrated;
        if (writtenToFsOffset < lastStart) {
            calibrated = lastStart;
        } else {
            calibrated = Math.min(writtenToFsOffset, diskEnd);
        }
        if (calibrated < cacheStartOffset) {
            return null;
        }
        delegate.truncateLastSegmentChannel(file, calibrated);
        Map<Long, Map<String, Long>> indexDeltas = new HashMap<>();
        // Last indexes: reopen + clear needPrepare + truncate to known-good; leave cache dirty.
        for (AsyncIndexFile indexFile : file.currentIndexFiles.values()) {
            try {
                FileCacheEntry indexEntry = indexFile.getCacheEntry();
                if (indexEntry == null || !indexEntry.isInitialized() || !indexEntry.fsInconsistent) {
                    continue;
                }
                long indexDiskSize = indexFile.openCurrentChannel();
                indexFile.needPrepare = false;
                final long written;
                final long cacheStart;
                synchronized (indexEntry) {
                    if (!indexEntry.isInitialized() || !indexEntry.fsInconsistent) {
                        continue;
                    }
                    written = indexEntry.writtenToFsOffset;
                    cacheStart = indexEntry.cacheStartOffset;
                }
                long indexCalibrated = Math.min(written, indexDiskSize);
                indexFile.channel.truncate(indexCalibrated);
                indexFile.channel.position(indexCalibrated);
                indexFile.pendingFsyncBytes = 0;
                if (indexCalibrated < cacheStart) {
                    // TODO: decide how to handle failed index align (invalidate / rebuild).
                    logger.warn("best-effort last index align skipped for {}, calibrated={} cacheStart={}",
                            indexFile.path, indexCalibrated, cacheStart);
                    continue;
                }
                indexDeltas.computeIfAbsent(lastStart, k -> new HashMap<>())
                        .put(indexFile.indexPrefix, indexCalibrated);
            } catch (Exception e) {
                // TODO: same as above — failure policy undecided; business may rebuild from segment.
                logger.warn("best-effort last index align failed for {} on {}",
                        indexFile.path, file.path, e);
            }
        }
        return Pair.of(calibrated, indexDeltas);
    }


    private <T> T awaitRestoreFuture(String path, CompletableFuture<T> future) {
        try {
            return future.get(restoreWaitTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException | InterruptedException e) {
            logger.warn("restore backing FS timed out or interrupted for {}, leaving inconsistent", path, e);
            return null;
        } catch (ExecutionException e) {
            logger.warn("restore backing FS failed for {}, leaving inconsistent", path, e);
            return null;
        }
    }

    /**
     * @return true if already consistent, or restore completed and deltas applied;
     *         false if cannot repair / incomplete / timeout / error (no apply).
     */
    private boolean restoreBackingFsAndAwait(AsyncFile file) {
        FileCacheEntry entry = file.getCacheEntry();

        if (entry != null && !entry.isInitialized() && entry.fsInconsistent) {
            synchronized (entry) {
                entry.fsInconsistent = false;
            }
        }

        if (!file.needPrepare) {
            if (entry == null || !entry.fsInconsistent) {
                return true;
            }
            if (entry.writtenToFsOffset < entry.cacheStartOffset) {
                return false;
            }
        }

        final boolean needApply = file.canWrite() && entry != null && entry.fsInconsistent;
        String id = file.ioKey;
        CompletableFuture<Long> restoreFuture = StorageUtil.supply(ioExecutor, () -> {
            StorageUtil.requireOpen(file);
            prepareFileSync(file);
            if (needApply) {
                return restoreFsConsistencySync(file);
            } else {
                return 0L;
            }
        });
        registerInFlight(id, restoreFuture);
        Long delta = awaitRestoreFuture(file.path, restoreFuture);
        if (delta == null) {
            return false;
        }
        if (needApply) {
            synchronized (entry) {
                if (entry.fsInconsistent) {
                    entry.pendingFsyncBytes = 0;
                    long oldWrittenToFsOffset = entry.writtenToFsOffset;
                    if (oldWrittenToFsOffset != delta) {
                        logger.warn("align offset after restoring {} from {} to {}",
                                file.path, oldWrittenToFsOffset, delta);
                        entry.writtenToFsOffset = delta;
                    }
                    entry.fsInconsistent = false;
                }
            }
        }
        return true;
    }

    /**
     * @return true if already consistent, or restore completed and deltas applied;
     *         false if cannot repair / incomplete / timeout / error (no apply).
     */
    private boolean restoreBackingFsAndAwait(AsyncSegmentFile file) {
        SegmentFileCacheEntry entry = file.getCacheEntry();

        if (entry != null && !entry.isInitialized() && entry.fsInconsistent) {
            synchronized (entry) {
                entry.fsInconsistent = false;
            }
        }

        if (!file.needPrepare) {
            if (entry == null || !entry.fsInconsistent) {
                return true;
            }
            SegmentDirState state = delegate.getSegmentDirState(file);
            if (!state.isEmpty()) {
                long lastStart = state.lastOffset;
                long pendingStart = Math.max(entry.writtenToFsOffset, lastStart);
                if (pendingStart < entry.cacheStartOffset) {
                    return false;
                }
            }
        }

        final boolean needApply = file.canWrite() && entry != null && entry.fsInconsistent;
        String id = file.ioKey;
        CompletableFuture<Pair<List<Long>, Map<Long, Map<String, Long>>>> restoreFuture =
                StorageUtil.supply(ioExecutor, () -> {
                    StorageUtil.requireOpen(file);
                    prepareFileSync(file);
                    if (needApply) {
                        return restoreSegmentFsConsistencySync(file);
                    } else {
                        return segmentRestoreDelta(0L, 0L, Collections.emptyMap());
                    }
                });
        registerInFlight(id, restoreFuture);
        Pair<List<Long>, Map<Long, Map<String, Long>>> delta =
                awaitRestoreFuture(file.path, restoreFuture);
        if (delta == null) {
            return false;
        }
        if (needApply) {
            List<Long> offsets = delta.getKey();
            Map<Long, Map<String, Long>> indexMap = delta.getValue();
            synchronized (entry) {
                if (entry.fsInconsistent) {
                    entry.pendingFsyncBytes = 0;
                    long restoredWrittenToFsOffset = offsets.get(0);
                    long oldWrittenToFsOffset = entry.writtenToFsOffset;
                    if (oldWrittenToFsOffset != restoredWrittenToFsOffset) {
                        logger.warn("align segment offset after restoring {} from {} to {}",
                                file.path, oldWrittenToFsOffset, restoredWrittenToFsOffset);
                        entry.writtenToFsOffset = restoredWrittenToFsOffset;
                    }
                    entry.localReadableFromOffset = offsets.get(1);
                    entry.fsInconsistent = false;
                }
            }
            if (indexMap != null && !indexMap.isEmpty()) {
                for (Map.Entry<Long, Map<String, Long>> byStart : indexMap.entrySet()) {
                    ConcurrentHashMap<String, FileCacheEntry> byPrefix = entry.indexFiles.get(byStart.getKey());
                    if (byPrefix == null) {
                        continue;
                    }
                    for (Map.Entry<String, Long> e : byStart.getValue().entrySet()) {
                        FileCacheEntry indexEntry = byPrefix.get(e.getKey());
                        if (indexEntry == null) {
                            continue;
                        }
                        synchronized (indexEntry) {
                            if (indexEntry.fsInconsistent) {
                                indexEntry.pendingFsyncBytes = 0;
                                long restoredWrittenToFsOffset = e.getValue();
                                long oldWrittenToFsOffset = indexEntry.writtenToFsOffset;
                                if (oldWrittenToFsOffset != restoredWrittenToFsOffset) {
                                    logger.warn("align index offset after restoring {} at {} prefix {} from {} to {}",
                                            file.path, byStart.getKey(), e.getKey(),
                                            oldWrittenToFsOffset, restoredWrittenToFsOffset);
                                    indexEntry.writtenToFsOffset = restoredWrittenToFsOffset;
                                }
                                indexEntry.fsInconsistent = false;
                            }
                        }
                    }
                }
            }
        }
        return true;
    }

    private void prepareFileAndAwait(AbstractStorageFile file) {
        if (!file.needPrepare) {
            return;
        }
        String id = file.ioKey;
        CompletableFuture<Void> prepareFuture = StorageUtil.run(ioExecutor, () -> {
            StorageUtil.requireOpen(file);
            prepareFileSync(file);
        });
        registerInFlight(id, prepareFuture);
        StorageUtil.awaitFuture(prepareFuture, file.path, ioWaitTimeoutMs, true);
    }

    // ---- AsyncFile ----

    @Override
    public CompletableFuture<AsyncFile> open(String path, AbstractStorageFile.OpenMode openMode, boolean atomicReplace, boolean lenient, String tenant) {
        return open(path, openMode, atomicReplace, lenient, tenant, null);
    }

    public CompletableFuture<AsyncFile> open(String path, AbstractStorageFile.OpenMode openMode, boolean atomicReplace, boolean lenient, String tenant, CacheMode cacheMode) {
        String key = StorageUtil.asyncFileKey(path);
        String ioKey = openMode.canWrite() ? key : allocateReaderIoKey(key);
        BackingFsMode fsMode = backingFsMode;
        if (fsMode != BackingFsMode.NO_FS) {
            // wait on the canonical writer key before delegate opens channels and initializes cache
            awaitInFlightIo(key, path, false);
        }
        return CompletableFuture.completedFuture(
                openFileSync(path, key, ioKey, openMode, atomicReplace, lenient, tenant, cacheMode, fsMode));
    }


    private AsyncFile openFileSync(String path, String key, String ioKey,
            AbstractStorageFile.OpenMode openMode, boolean atomicReplace, boolean lenient, String tenant,
            CacheMode cacheModeOverride, BackingFsMode fsMode) {
        final boolean noFs = fsMode == BackingFsMode.NO_FS;
        CacheMode cacheMode = resolveFileCacheMode(atomicReplace, cacheModeOverride);
        if (noFs && cacheMode == CacheMode.NO_CACHE) {
            throw new IllegalArgumentException("NO_CACHE is not supported when backing FS mode is NO_FS");
        }
        AbstractStorageFile.OpenMode effectiveOpenMode = openMode;
        if (openMode == AbstractStorageFile.OpenMode.WRITE && cacheMode == CacheMode.FULL_CACHE) {
            effectiveOpenMode = AbstractStorageFile.OpenMode.READ_WRITE;
        }
        AsyncFile file = delegate.openSync(path, key, ioKey, effectiveOpenMode,
                atomicReplace, lenient, tenant, noFs);
        delegate.openWithFileEntry(file, noFs, this::registerInFlight, this::scheduleCloseChannels,
                restoreWaitTimeoutMs, ioWaitTimeoutMs);
        file.cacheMode = cacheMode;
        if (cacheMode != CacheMode.NO_CACHE) {
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
                cleanupOpenFailed(file);
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

            try {
                initFileCache(file, first, fsMode == BackingFsMode.NO_CACHE, noFs);
            } catch (Throwable t) {
                logger.error("init file cache failed for {}, closing file", file.path, t);
                cleanupOpenFailed(file);
                throw t;
            }
        }
        return file;
    }

    private void cleanupOpenFailed(AsyncFile file) {
        cleanupOpenFailed(file, delegate::closeSync);
    }

    private void cleanupOpenFailed(AsyncSegmentFile file) {
        cleanupOpenFailed(file, delegate::closeSync);
    }

    private <T extends AbstractStorageFile> void cleanupOpenFailed(T file,
            java.util.function.Function<T, List<FileChannel>> closeSync) {
        try {
            scheduleCloseChannels(file.path, closeSync.apply(file));
        } catch (Throwable t) {
            logger.error("closeSync failed during open cleanup for {}", file.path, t);
        }
        try {
            file.onCacheClose.run();
        } catch (Throwable t) {
            logger.error("failed to release cache entry for {}", file.path, t);
        }
    }

    private void initCache(FileCacheEntry entry, boolean first, Runnable init) {
        if (entry.initDone.getCount() == 0) {
            // Already initialized by an earlier call — repeated init is a no-op.
            return;
        }
        if (first) {
            try {
                init.run();
            } finally {
                entry.initDone.countDown();
            }
        } else {
            try {
                entry.initDone.await();
            } catch (Throwable t) {
                logger.warn("await cache init failed", t);
            }
        }
    }

    private void initFileCache(AbstractStorageFile file, boolean first, boolean noCache, boolean noFs,
            java.util.function.Supplier<Long> backingEndOffsetSupplier, Runnable initFromFs) {
        initStorageCache(file, first, noCache, initFromFs);

        FileCacheEntry entry = file.getCacheEntry();
        if (first || !file.canWrite() || file.atomicReplace || noFs || entry == null
                || !entry.isInitialized() || entry.fsInconsistent) {
            return;
        }

        long backingEndOffset = awaitIoCachePrep(file, null, ioWaitTimeoutMs,
                backingEndOffsetSupplier, null);
        synchronized (entry) {
            if (!entry.isInitialized() || entry.fsInconsistent) {
                return;
            }
            if (backingEndOffset < entry.cacheStartOffset || backingEndOffset > entry.cacheEndOffset) {
                throw new CacheChunksNotContinuousException(
                        "backing end offset " + backingEndOffset + " for " + file.path
                                + " is outside cache range [" + entry.cacheStartOffset
                                + ", " + entry.cacheEndOffset + "]");
            }
            long oldWrittenToFsOffset = entry.writtenToFsOffset;
            if (oldWrittenToFsOffset != backingEndOffset) {
                logger.warn("align shared writer offset for {} from {} to backing end {}, cache range=[{}, {}]",
                        file.path, oldWrittenToFsOffset, backingEndOffset,
                        entry.cacheStartOffset, entry.cacheEndOffset);
                entry.writtenToFsOffset = backingEndOffset;
            }
        }
    }

    private void initFileCache(AsyncFile file, boolean first, boolean noCache, boolean noFs) {
        initFileCache(file, first, noCache, noFs, () -> delegate.sizeSync(file), () -> {
            if (file.cacheMode == CacheMode.TAIL_CACHE) {
                initTailCacheSync(file, () -> delegate.sizeSync(file), true);
            }
            if (file.cacheMode == CacheMode.FULL_CACHE) {
                loadFullFileCache(file, false, true);
            }
        });
    }

    private void initSegmentCache(AsyncSegmentFile file, boolean first, boolean noCache, boolean noFs) {
        initFileCache(file, first, noCache, noFs, () -> segmentExclusiveEndOffset(file),
                () -> initTailCacheSync(file, () -> segmentExclusiveEndOffset(file), true));
    }

    private void initStorageCache(AbstractStorageFile file, boolean first, boolean noCache, Runnable initFromFs) {
        boolean effectiveNoCache = noCache && !file.needPrepare;
        initCache(file.cacheEntry, first, () -> {
            try {
                if (!useCache(file, effectiveNoCache) || file.cacheEntry.isInitialized()) return;
                if (file.needPrepare) {
                    FileCacheEntry entry = file.cacheEntry;
                    synchronized (entry) {
                        if (!entry.isInitialized()) {
                            entry.cacheStartOffset = 0;
                            entry.fsInconsistent = true;
                        }
                    }
                    return;
                }
                initFromFs.run();
            } catch (Exception e) {
                logger.warn("init cache failed for {}", file.path, e);
            }
        });
    }

    private void initTailCacheSync(AbstractStorageFile file,
            java.util.function.Supplier<Long> endOffsetSupplier, boolean timeBounded) {
        long endOffset = timeBounded
                ? awaitIoCachePrep(file, null, ioWaitTimeoutMs, endOffsetSupplier, null)
                : executeWithIoFailureHandling(file, endOffsetSupplier);
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

    private <T> T awaitIoCachePrep(AbstractStorageFile file, String registerKey, long timeoutMs,
            java.util.function.Supplier<T> task, java.util.function.Consumer<T> clean) {
        return StorageUtil.awaitIoCachePrep(ioExecutor, file, registerKey, timeoutMs,
                this::registerInFlight, task, clean);
    }

    private void loadFullFileCache(AsyncFile file, boolean memoryAllocateBlocking,
            boolean timeBounded) {
        long reservedBytes = 0;
        ByteBuf fileData = null;
        Map<Long, ByteBuf> allocated = new HashMap<>();
        long actualSize;
        FileCacheEntry entry = file.cacheEntry;
        try {
            long initialSize = timeBounded
                    ? awaitIoCachePrep(file, null, ioWaitTimeoutMs, () -> delegate.sizeSync(file), null)
                    : executeWithIoFailureHandling(file, () -> delegate.sizeSync(file));
            long initialCapacity = file.atomicReplace
                    ? initialSize
                    : StorageUtil.chunkCapacityForBytes(initialSize, chunkSize);
            if (initialCapacity > maxCacheSizePerFileBytes) {
                throw new CacheFileTooLargeException(file.path, initialCapacity);
            }
            if (memoryAllocateBlocking) {
                memoryTracker.reserve(initialCapacity, maxCacheSizeBytes, ioWaitTimeoutMs);
            } else if (!memoryTracker.tryReserve(initialCapacity, maxCacheSizeBytes)) {
                throw new CacheMemoryReserveException(initialCapacity, maxCacheSizeBytes, memoryTracker.committedBytes());
            }
            reservedBytes = initialCapacity;

            Pair<Boolean, ByteBuf> fullData = timeBounded
                    ? awaitIoCachePrep(file, null, ioWaitTimeoutMs, () -> readFullData(file, initialSize),
                            result -> result.getValue().release())
                    : executeWithIoFailureHandling(file, () -> readFullData(file, initialSize));
            boolean aligned = fullData.getKey();
            fileData = fullData.getValue();
            actualSize = fileData.readableBytes();
            long actualCapacity = file.atomicReplace
                    ? actualSize
                    : StorageUtil.chunkCapacityForBytes(actualSize, chunkSize);
            if (actualCapacity > maxCacheSizePerFileBytes) {
                throw new CacheFileTooLargeException(file.path, actualCapacity);
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
                long chunkIdx = 0;
                while (fileData.isReadable()) {
                    ByteBuf chunk;
                    try {
                        chunk = StorageAllocator.ALLOC.directBuffer((int) chunkSize);
                    } catch (Throwable e) {
                        throw new CacheMemoryReserveException(chunkSize, e);
                    }
                    allocated.put(chunkIdx, chunk);
                    int length = (int) Math.min(chunkSize, fileData.readableBytes());
                    chunk.setBytes(0, fileData, length);
                    chunkIdx++;
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
            data = delegate.readSync(file, fileSize, 0, 0);
        } else if (fileSize <= preloadChunkThreshold * chunkSize) {
            // small file: aligned read — buffer capacity rounded up to chunkSize multiples,
            // so each chunk slice maps directly onto an aligned region without copying.
            data = delegate.readSync(file, fileSize, 0, chunkSize);
        } else {
            // large file: single read, copy into per-chunk buffers
            aligned = false;
            data = delegate.readSync(file, fileSize, 0, 0);
        }
        return new Pair<>(aligned, data);
    }

    @Override
    public CompletableFuture<Boolean> isFile(AsyncFile file) {
        if (backingFsMode == BackingFsMode.NO_FS) {
            if (file.needPrepare) {
                return CompletableFuture.completedFuture(true);
            }
            return CompletableFuture.completedFuture(file.channel != null);
        }
        return delegate.isFile(file);
    }

    @Override
    public CompletableFuture<Boolean> isDirectory(String path) {
        if (backingFsMode == BackingFsMode.NO_FS) {
            throw new CannotDetermineInNoFsException("isDirectory(" + path + ")");
        }
        return delegate.isDirectory(path);
    }

    @Override
    public CompletableFuture<Long> lastModified(AsyncFile file) {
        return lastModifiedOf(backingFsMode == BackingFsMode.NO_FS, file, () -> delegate.lastModified(file));
    }

    @Override
    public CompletableFuture<Void> position(AsyncFile file, long position) {
        if (!file.canRead()) {
            throw new IllegalArgumentException("position() requires read mode");
        }
        StorageUtil.requireOpen(file);
        if (backingFsMode != BackingFsMode.NO_FS) {
            awaitInFlightIo(file.ioKey, file.path, false);
        }
        try {
            delegate.positionSync(file, position);
            return CompletableFuture.completedFuture(null);
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }


    @Override
    public CompletableFuture<ByteBuf> read(AsyncFile file, long length, long offset) {
        return readInternal(file, length, offset, false,
                () -> true,
                () -> executeWithIoFailureHandling(file, () -> delegate.readSync(file, length, offset, 0)));
    }

    @Override
    public CompletableFuture<ByteBuf> read(AsyncFile file, long length) {
        long readOffset = file.position;
        return readInternal(file, length, 0, true,
                () -> true,
                () -> executeWithIoFailureHandling(file, () -> delegate.readSync(file, length, readOffset, 0)));
    }

    private CompletableFuture<ByteBuf> readInternal(AbstractStorageFile file, long length, long offset,
            boolean fromPosition, java.util.function.BooleanSupplier fsPrepare,
            java.util.function.Supplier<ByteBuf> fsRead) {
        StorageUtil.requireOpen(file);
        final BackingFsMode fsMode = backingFsMode;
        FileCacheEntry entry = file.getCacheEntry();
        long readOffset = fromPosition ? file.position : offset;
        Pair<Boolean, Boolean> decision = preferCacheRead(file, entry, readOffset, readPreferCache, fsMode);
        if (decision.getKey()) {
            ByteBuf cached = null;
            synchronized (entry) {
                if (readOffset >= entry.cacheStartOffset && entry.isInitialized()) {
                    cached = entry.readWithCache(length, readOffset, file.atomicReplace, chunkSize);
                }
            }
            if (cached != null) {
                if (fromPosition) {
                    file.position = readOffset + cached.readableBytes();
                }
                return CompletableFuture.completedFuture(cached);
            }
        }

        if (!decision.getValue()) {
            throw new CannotReadPositionInNoFsException(file.path, offset);
        }

        final String ioKey = file.ioKey;
        try {
            awaitInFlightIo(ioKey, file.path, false);
        } catch (Exception e) {
            return CompletableFuture.completedFuture(Unpooled.buffer(0));
        }

        if (!fsPrepare.getAsBoolean()) {
            return CompletableFuture.completedFuture(Unpooled.buffer(0));
        }

        CompletableFuture<ByteBuf> ioFuture = StorageUtil.supply(ioExecutor, () -> {
            StorageUtil.requireOpen(file);
            prepareFileSync(file);
            ByteBuf buf = fsRead.get();
            if (fromPosition) {
                file.position = readOffset + buf.readableBytes();
            }
            return buf;
        });
        registerInFlight(ioKey, ioFuture);
        return ioFuture;
    }


    // Returns (preferCache, canDegradeToDisk). Throws when neither cache nor disk is usable.
    Pair<Boolean, Boolean> preferCacheRead(AbstractStorageFile file, FileCacheEntry entry, long offset,
            boolean preferCache, BackingFsMode fsMode) {
        boolean inCache = file.cacheMode != CacheMode.NO_CACHE
                && entry != null
                && entry.isInitialized()
                && offset >= entry.cacheStartOffset;
        boolean localReadable = fsMode != BackingFsMode.NO_FS
                && (entry == null
                        || (!entry.fsInconsistent && offset >= entry.localReadableFromOffset));
        if (inCache) {
            if (!localReadable) {
                return Pair.of(true, false);
            }
            if (!preferCache || fsMode == BackingFsMode.NO_CACHE) {
                if (file.atomicReplace) {
                    return Pair.of(entry.cacheGen != entry.writtenGen, true);
                }
                return Pair.of(offset >= entry.writtenToFsOffset, true);
            }
            return Pair.of(true, true);
        }
        if (!localReadable) {
            throw new CannotReadPositionInNoFsException(file.path, offset);
        }
        return Pair.of(false, true);
    }

    long transferToByCache(java.util.List<ByteBuf> slices, WritableByteChannel target) throws IOException {
        if (slices.isEmpty()) {
            return 0L;
        }
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

    // Must be called only when there is no in-flight IO for this file.
    private ByteBuf buildWriteBufAfterInFlight(FileCacheEntry entry) {
        long pending = Math.max(0, entry.cacheEndOffset - entry.writtenToFsOffset);
        if (pending < writeBatchBytes) {
            return Unpooled.buffer(0);
        }
        return entry.buildWriteBufFromCache(maxWriteChunkThreshold * chunkSize, chunkSize);
    }

    @Override
    public CompletableFuture<Long> write(AsyncFile file, ByteBuf data) {
        if (!file.canWrite()) {
            data.release();
            throw new IllegalArgumentException("operation requires write mode: " + file.path);
        }
        if (file.closed) {
            data.release();
            throw new IllegalStateException("file is closed: " + file.path);
        }
        if (file.atomicReplace && data.readableBytes() == 0) {
            data.release();
            throw new IllegalArgumentException("atomic replace requires non-empty data: " + file.path);
        }
        try {
            file.throwIfNoSpace();
        } catch (RuntimeException e) {
            data.release();
            throw e;
        }
        final BackingFsMode fsMode = backingFsMode;
        final boolean noFs = fsMode == BackingFsMode.NO_FS;
        return writeInternal(file, data, fsMode,
                () -> initCacheAndAppend(file, data, noFs),
                writeBuf -> executeWithIoFailureHandling(file, () -> delegate.writeSync(file, writeBuf)),
                () -> executeWithIoFailureHandling(file, () -> {
                    delegate.fsyncSync(file);
                    return null;
                }),
                () -> restoreBackingFsAndAwait(file));
    }

    private CompletableFuture<Long> writeInternal(AbstractStorageFile file, ByteBuf data,
            BackingFsMode fsMode,
            Runnable initCacheAndAppend,
            java.util.function.Function<ByteBuf, Long> fsWrite,
            Runnable fsFsync,
            java.util.function.Supplier<Boolean> restoreBackingFs) {
        FileCacheEntry entry = file.getCacheEntry();
        final long writeSize = data.readableBytes();
        final boolean noFs = fsMode == BackingFsMode.NO_FS;
        final boolean useCache = useCache(file, fsMode == BackingFsMode.NO_CACHE);
        final String id = file.ioKey;
        if (useCache && !entry.isInitialized()) {
            if (noFs) {
                data.release();
                throw new CannotInitCacheInNoFsException(file.path);
            }
            try {
                if (hasInFlightIo(id)) {
                    awaitInFlightIo(id, file.path, false);
                }
            } catch (Exception e) {
                data.release();
                throw e;
            }
        }

        if (useCache) {
            try {
                initCacheAndAppend.run();
            } catch (Exception e) {
                data.release();
                throw e;
            }
        }
        if (!useCache && noFs) {
            data.release();
            throw new CannotWriteWithoutCacheInNoFsException(file.path,
                    "cache is not enabled");
        }
        file.lastModified = System.currentTimeMillis();
        if (!noFs) {
            boolean prepareFailed = false;
            Exception prepareError = null;
            try {
                if (hasInFlightIo(id)) {
                    if (!useCache || file.atomicReplace || entry.fsInconsistent) {
                        awaitInFlightIo(id, file.path, false);
                    } else {
                        data.release();
                        return CompletableFuture.completedFuture(writeSize);
                    }
                }
                if (!restoreBackingFs.get()) {
                    prepareFailed = true;
                }
            } catch (Exception e) {
                prepareFailed = true;
                prepareError = e;
            }
            if (prepareFailed) {
                data.release();
                if (useCache && !file.atomicReplace) {
                    if (prepareError != null) {
                        logger.warn("failed to prepare backing FS for {}, data remains in cache", file.path, prepareError);
                    } else {
                        logger.warn("failed to prepare backing FS for {}, data remains in cache", file.path);
                    }
                    return CompletableFuture.completedFuture(writeSize);
                }
                if (prepareError != null) {
                    if (prepareError instanceof OperationNotExecutedException) {
                        throw (OperationNotExecutedException) prepareError;
                    }
                    throw new OperationNotExecutedException(file.path, prepareError);
                }
                throw new OperationNotExecutedException(file.path);
            }

            if (!useCache && entry != null && entry.isInitialized()) {
                try {
                    flushPendingWriteAndAwait(file, fsWrite, fsFsync, true);
                } catch (RuntimeException e) {
                    data.release();
                    throw e;
                }
                entry.reset();
            }
        }

        final ByteBuf writeBuf;
        final long atomicIoGen;
        if (!useCache) {
            writeBuf = data;
            atomicIoGen = 0;
        } else if (file.atomicReplace) {
            writeBuf = data;
            atomicIoGen = entry.cacheGen;
        } else {
            if (entry.fsInconsistent) {
                data.release();
                writeBuf = Unpooled.buffer(0);
            } else {
                long pending = entry.cacheEndOffset - entry.writtenToFsOffset;
                if (pending == writeSize) {
                    if (pending >= writeBatchBytes) {
                        writeBuf = data;
                    } else {
                        data.release();
                        writeBuf = Unpooled.buffer(0);
                    }
                } else {
                    try {
                        writeBuf = buildWriteBufAfterInFlight(entry);
                    } catch (CacheChunksNotContinuousException e) {
                        // Should be unreachable. if it happens, it means the cache is corrupted and memory will grow until reserve fails.
                        logger.error("cache chunks are not continuous for {}, data remains in cache", file.path, e);
                        return CompletableFuture.completedFuture(writeSize);
                    } finally {
                        data.release();
                    }
                }
            }
            atomicIoGen = 0;
        }
        if (!writeBuf.isReadable() || noFs) {
            writeBuf.release();
            return CompletableFuture.completedFuture(writeSize);
        }
        CompletableFuture<Long> ioFuture = StorageUtil.supply(ioExecutor, () -> {
            if (file.closed) {
                writeBuf.release();
                throw new IllegalStateException("file is closed: " + file.path);
            }
            long written = fsWrite.apply(writeBuf);
            if (useCache) {
                synchronized (entry) {
                    if (!entry.fsInconsistent) {
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
            }
            return writeSize;
        }, writeBuf);
        registerInFlight(id, ioFuture);
        if (!useCache) {
            return ioFuture;
        }
        return CompletableFuture.completedFuture(writeSize);
    }

    private boolean useCache(AbstractStorageFile file, boolean noCache) {
        return !noCache && file.cacheMode != CacheMode.NO_CACHE;
    }

    private void initCacheAndAppend(AsyncFile file, ByteBuf data, boolean noFs) {
        FileCacheEntry entry = file.getCacheEntry();

        ByteBuf view = data.duplicate();
        if (file.atomicReplace) {
            replaceAtomicCache(file, entry, view);
            return;
        }
        if (file.cacheMode == CacheMode.FULL_CACHE) {
            initFullCacheAndAppend(file, entry, view, noFs);
            return;
        }
        initTailCacheAndAppend(file, entry, view, noFs,
                () -> delegate.sizeSync(file));
    }

    private void initCacheAndAppend(AsyncSegmentFile file, ByteBuf data, boolean noFs) {
        FileCacheEntry entry = file.getCacheEntry();
        initTailCacheAndAppend(file, entry, data.duplicate(), noFs,
                () -> segmentExclusiveEndOffset(file));
    }

    private void initCacheAndAwait(AbstractStorageFile file, Runnable init) {
        CompletableFuture<Void> initFuture = StorageUtil.run(ioExecutor, () -> {
            StorageUtil.requireOpen(file);
            init.run();
        });
        registerInFlight(file.ioKey, initFuture);
        StorageUtil.awaitFuture(initFuture, file.path, ioWaitTimeoutMs, true);
    }

    private void initFullCacheAndAppend(AsyncFile file, FileCacheEntry entry, ByteBuf data,
            boolean noFs) {
        if (!entry.isInitialized()) {
            initCacheAndAwait(file, () -> loadFullFileCache(file, true, false));
        }
        appendToChunkedCache(file, entry, data, false, noFs);
    }

    private void initTailCacheAndAppend(AbstractStorageFile file, FileCacheEntry entry, ByteBuf data,
            boolean noFs, java.util.function.Supplier<Long> endOffsetSupplier) {
        if (!entry.isInitialized()) {
            initCacheAndAwait(file, () -> initTailCacheSync(file, endOffsetSupplier, false));
        }
        appendToChunkedCache(file, entry, data, true, noFs);
    }

    private void appendToChunkedCache(AbstractStorageFile file, FileCacheEntry entry, ByteBuf data,
            boolean tailCache, boolean noFs) {
        if (!data.isReadable()) {
            return;
        }

        final long nowNanos = System.nanoTime();
        final long newFirst;
        final int newChunkCount;
        final long newBytes;
        synchronized (entry) {
            if (!noFs && !entry.fsInconsistent
                    && entry.writtenToFsOffset < entry.cacheStartOffset) {
                throw new CacheChunksNotContinuousException(
                        "written offset " + entry.writtenToFsOffset + " for " + file.path
                                + " is before cache start " + entry.cacheStartOffset);
            }

            long startOffset = entry.cacheEndOffset;
            long endOffset = startOffset + data.readableBytes();
            long first = startOffset / chunkSize;
            long last = (endOffset - 1) / chunkSize;
            newFirst = entry.chunks.containsKey(first) ? first + 1 : first;
            newChunkCount = (int) (last - newFirst + 1);
            newBytes = newChunkCount * chunkSize;

            if (tailCache) {
                if (newChunkCount > 0) {
                    evictTailBeforeAppend(file.getKey(), entry, newChunkCount, nowNanos,
                            noFs || entry.fsInconsistent);
                }
            } else if (entry.bodySizeBytes + newBytes > maxCacheSizePerFileBytes) {
                throw new CacheFileTooLargeException(file.path, entry.bodySizeBytes + newBytes);
            }
        }

        ByteBuf[] bufs = new ByteBuf[newChunkCount];
        if (newChunkCount > 0) {
            memoryTracker.reserve(newBytes, maxCacheSizeBytes, ioWaitTimeoutMs);
            try {
                for (int j = 0; j < newChunkCount; j++) {
                    bufs[j] = StorageAllocator.ALLOC.directBuffer((int) chunkSize);
                }
            } catch (Throwable t) {
                for (ByteBuf buf : bufs) {
                    if (buf != null) {
                        buf.release();
                    }
                }
                memoryTracker.release(newBytes);
                throw new CacheMemoryReserveException(newBytes, t);
            }
        }

        synchronized (entry) {
            for (int j = 0; j < newChunkCount; j++) {
                entry.putChunk(newFirst + j, new CacheChunk(bufs[j]));
            }
            entry.appendToChunkedCache(data, nowNanos, chunkSize);
        }
    }

    private void replaceAtomicCache(AsyncFile file, FileCacheEntry entry, ByteBuf data) {
        int length = data.readableBytes();
        if (length > maxCacheSizePerFileBytes) {
            throw new CacheFileTooLargeException(file.path, length);
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
        newBuffer.setBytes(0, data, length);
        synchronized (entry) {
            entry.setAtomicChunk(new CacheChunk(newBuffer), 0);
        }
    }

    private void evictTailBeforeAppend(String fileKey, FileCacheEntry entry, int newChunks, long nowNanos,
            boolean allowDirtyEvict) {
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
            if (chunkEnd > durableFsOffset) {
                if (allowDirtyEvict) {
                    if (!entry.fsInconsistent) {
                        entry.fsInconsistent = true;
                        logger.warn("{} has missing data", fileKey);
                    }
                } else {
                    durableLimit = true;
                    break;
                }
            }
            if (chunk.lastAppendNanos > expireBeforeNanos) break;
            evicted++;
            index++;
            chunkEnd += chunkSize;
        }
        if (!durableLimit) {
            while (evicted < minEvict) {
                if (chunkEnd > durableFsOffset) {
                    if (allowDirtyEvict) {
                        if (!entry.fsInconsistent) {
                            entry.fsInconsistent = true;
                            logger.warn("{} has missing data", fileKey);
                        }
                    } else {
                        break;
                    }
                }
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

    @Override
    public CompletableFuture<Void> delete(String path) {
        if (backingFsMode == BackingFsMode.NO_FS) {
            logger.warn("skip delete {} when backing FS mode is NO_FS", path);
            return CompletableFuture.completedFuture(null);
        }
        return delegate.delete(path);
    }


    @Override
    public CompletableFuture<Void> delete(AsyncFile file) {
        StorageUtil.requireWriteMode(file);
        StorageUtil.requireOpen(file);
        final boolean noFs = backingFsMode == BackingFsMode.NO_FS;
        final String id = file.ioKey;
        FileCacheEntry entry = file.getCacheEntry();
        if (noFs && (entry == null || !entry.isInitialized())) {
            throw new CannotWriteWithoutCacheInNoFsException(file.path, "file cache is not initialized");
        }
        if (noFs) {
            logger.warn("skip delete {} when backing FS mode is NO_FS", file.path);
        } else {
            awaitInFlightIo(id, file.path, false);
            file.throwIfNoSpace();
        }
        if (entry != null) {
            synchronized (entry) {
                if (entry.isInitialized()) {
                    entry.clear();
                    if (noFs) {
                        entry.fsInconsistent = true;
                    }
                }
            }
        }
        if (noFs) {
            return CompletableFuture.completedFuture(null);
        }
        return StorageUtil.run(ioExecutor, () -> {
            StorageUtil.requireOpen(file);
            delegate.deleteSync(file.path);
        });
    }

    @Override
    public CompletableFuture<Boolean> exists(String path) {
        if (backingFsMode == BackingFsMode.NO_FS) {
            throw new CannotDetermineInNoFsException("exists(" + path + ")");
        }
        return delegate.exists(path);
    }

    @Override
    public CompletableFuture<Long> size(AsyncFile file) {
        StorageUtil.requireOpen(file);
        final boolean noFs = backingFsMode == BackingFsMode.NO_FS;
        if (file.cacheMode != CacheMode.NO_CACHE) {
            FileCacheEntry entry = file.getCacheEntry();
            synchronized (entry) {
                if (entry.isInitialized()) {
                    return CompletableFuture.completedFuture(
                            Math.max(entry.writtenToFsOffset, entry.cacheEndOffset));
                }
            }
        }
        if (noFs) {
            throw new CannotDetermineInNoFsException("size(" + file.path + ")");
        }
        return StorageUtil.supply(ioExecutor, () -> {
            StorageUtil.requireOpen(file);
            return executeWithIoFailureHandling(file, () -> delegate.sizeSync(file));
        });
    }


    @Override
    public CompletableFuture<Boolean> mkdir(String path, boolean recursive) {
        if (backingFsMode == BackingFsMode.NO_FS) {
            logger.warn("skip mkdir {} when backing FS mode is NO_FS", path);
            return CompletableFuture.completedFuture(true);
        }
        return StorageUtil.supply(ioExecutor, () -> delegate.mkdirSync(path, recursive));
    }

    @Override
    public CompletableFuture<Boolean> rmdir(String path, boolean recursive) {
        if (backingFsMode == BackingFsMode.NO_FS) {
            logger.warn("skip rmdir {} when backing FS mode is NO_FS", path);
            return CompletableFuture.completedFuture(true);
        }
        return delegate.rmdir(path, recursive);
    }

    @Override
    public CompletableFuture<Void> truncate(AsyncFile file, long size) {
        StorageUtil.requireWriteMode(file);
        StorageUtil.requireOpen(file);
        final boolean noFs = backingFsMode == BackingFsMode.NO_FS;
        final String id = file.ioKey;
        FileCacheEntry entry = file.getCacheEntry();
        if (!noFs) {
            awaitInFlightIo(id, file.path, false);
            file.throwIfNoSpace();
            prepareFileAndAwait(file);
        } else {
            if (entry == null || !entry.isInitialized()) {
                throw new CannotWriteWithoutCacheInNoFsException(file.path, "file cache is not initialized");
            }
        }
        if (entry != null) {
            synchronized (entry) {
                if (entry.isInitialized()) {
                    if (size >= entry.cacheEndOffset) {
                        return CompletableFuture.completedFuture(null);
                    }
                    if (file.atomicReplace) {
                        ByteBuf newChunk;
                        try {
                            newChunk = StorageAllocator.ALLOC.directBuffer((int) size);
                        } catch (Throwable e) {
                            throw new CacheMemoryReserveException(size, e);
                        }
                        CacheChunk oldChunk = entry.chunks.get(0L);
                        newChunk.setBytes(0, oldChunk.buffer, 0, (int) size);
                        entry.setAtomicChunk(new CacheChunk(newChunk), Math.min(size, entry.writtenToFsOffset));
                    } else {
                        entry.truncateTo(size, chunkSize);
                    }
                    if (noFs) {
                        entry.fsInconsistent = true;
                    }
                }
            }
        }
        file.lastModified = System.currentTimeMillis();
        if (noFs || (entry != null && entry.fsInconsistent)) {
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Void> ioFuture = StorageUtil.run(ioExecutor, () -> {
            StorageUtil.requireOpen(file);
            executeWithIoFailureHandling(file, () -> {
                delegate.truncateSync(file, size);
                return null;
            });
            if (entry != null) {
                synchronized (entry) {
                    if (!entry.fsInconsistent) {
                        entry.pendingFsyncBytes = file.pendingFsyncBytes;
                    }
                }
            }
        });
        registerInFlight(id, ioFuture);
        return ioFuture;
    }


    @Override
    public CompletableFuture<Void> close(AsyncFile file) {
        if (!file.canCloseByUser) {
            throw new IllegalStateException("close is not allowed for: " + file.path);
        }
        return closeInternal(file,
                () -> delegate.closeSync(file),
                f -> asyncFileFlushPendingWriteAndAwait(f, false),
                () -> restoreBackingFsAndAwait(file));
    }

    private <T extends AbstractStorageFile> CompletableFuture<Void> closeInternal(T file,
            java.util.function.Supplier<List<FileChannel>> fsClose, java.util.function.Consumer<T> flush,
            java.util.function.Supplier<Boolean> restoreBackingFs) {
        if (file.closed) {
            return CompletableFuture.completedFuture(null);
        }
        boolean noFs = backingFsMode == BackingFsMode.NO_FS;
        final boolean noSpaceBeforeClose = file.noSpaceFailure != null;
        if (noSpaceBeforeClose) {
            logger.warn("skip flush while closing {} after ENOSPC", file.path);
            noFs = true;
        }
        final String id = file.ioKey;
        if (!noFs) {
            awaitInFlightIo(id, file.path, false);
            if (!restoreBackingFs.get()) {
                logger.warn("failed to restore backing FS while closing {}, falling back to NO_FS close", file.path);
                noFs = true;
            }
            // restore may have returned false on timeout, leaving its task running
            // close should wait until io task is completed inf under fs mode.
            awaitInFlightIo(id, file.path, false);
            if (!noFs) {
                flush.accept(file);
            }
        }
        if (noFs && file.canWrite()) {
            FileCacheEntry entry = file.getCacheEntry();
            if (entry != null && (entry.fsInconsistent || entry.isCacheDirty(file.atomicReplace) || entry.isFsyncDirty())) {
                logger.warn("{} may have data loss", file.path);
            }
        }
        
        List<FileChannel> channels;
        try {
            channels = fsClose.get();
        } finally {
            file.onCacheClose.run();
        }
        scheduleCloseChannels(file.path, channels);
        return CompletableFuture.completedFuture(null);
    }


    @Override
    public CompletableFuture<Void> fsync(AsyncFile file) {
        return fsyncInternal(file,
                () -> asyncFileFlushPendingWriteAndAwait(file, false),
                () -> executeWithIoFailureHandling(file, () -> {
                    delegate.fsyncSync(file);
                    return null;
                }),
                () -> restoreBackingFsAndAwait(file));
    }

    private CompletableFuture<Void> fsyncInternal(AbstractStorageFile file, Runnable flushPending, Runnable fsFsync,
            java.util.function.Supplier<Boolean> restoreBackingFs) {
        StorageUtil.requireWriteMode(file);
        StorageUtil.requireOpen(file);
        final boolean noFs = backingFsMode == BackingFsMode.NO_FS;
        final FileCacheEntry entry = file.getCacheEntry();
        if (noFs) {
            return CompletableFuture.completedFuture(null);
        }
        final String id = file.ioKey;
        awaitInFlightIo(id, file.path, false);
        file.throwIfNoSpace();
        if (!restoreBackingFs.get()) {
            logger.warn("failed to restore backing FS while fsyncing {}, skipping fsync", file.path);
            return CompletableFuture.completedFuture(null);
        }
        flushPending.run();

        if (entry == null || !entry.isInitialized()) {
            CompletableFuture<Void> ioFuture = StorageUtil.run(ioExecutor, () -> {
                StorageUtil.requireOpen(file);
                fsFsync.run();
                if (entry != null) {
                    synchronized (entry) {
                        if (!entry.fsInconsistent) {
                            entry.pendingFsyncBytes = file.pendingFsyncBytes;
                        }
                    }
                }
            });
            registerInFlight(id, ioFuture);
            return ioFuture;
        }

        return CompletableFuture.completedFuture(null);
    }


    @Override
    public CompletableFuture<List<String>> list(String path) {
        if (backingFsMode == BackingFsMode.NO_FS) {
            throw new CannotDetermineInNoFsException("list(" + path + ")");
        }
        return delegate.list(path);
    }

    @Override
    public CompletableFuture<Long> transferTo(AsyncFile file, long position, long count, WritableByteChannel target) {
        StorageUtil.requireOpen(file);
        return transferToInternal(file, position, count, target,
                () -> true,
                () -> executeWithIoFailureHandling(file,
                        () -> delegate.transferToSync(file, position, count, target)));
    }

    private CompletableFuture<Long> transferToInternal(AbstractStorageFile file, long offset, long count,
            WritableByteChannel target, java.util.function.BooleanSupplier fsPrepare,
            java.util.function.Supplier<Long> fsTransfer) {
        StorageUtil.requireOpen(file);
        final BackingFsMode fsMode = backingFsMode;
        FileCacheEntry entry = file.getCacheEntry();
        Pair<Boolean, Boolean> decision = preferCacheRead(file, entry, offset, transferPreferCache, fsMode);
        if (decision.getKey()) {
            java.util.List<ByteBuf> slices = null;
            synchronized (entry) {
                if (offset >= entry.cacheStartOffset && entry.isInitialized()) {
                    long end = Math.min(offset + count, entry.cacheEndOffset);
                    slices = entry.collectCacheSlices(offset, end, false, file.atomicReplace, chunkSize);
                }
            }
            if (slices != null) {
                try {
                    return CompletableFuture.completedFuture(transferToByCache(slices, target));
                } catch (IOException e) {
                    return CompletableFuture.failedFuture(new SocketErrorException(e));
                }
            }
        }

        if (!decision.getValue()) {
            throw new CannotReadPositionInNoFsException(file.path, offset);
        }

        // Barrier before fsPrepare — see readInternal for why the order matters.
        final String ioKey = file.ioKey;
        try {
            awaitInFlightIo(ioKey, file.path, false);
        } catch (Exception e) {
            return CompletableFuture.completedFuture(0L);
        }

        if (!fsPrepare.getAsBoolean()) {
            return CompletableFuture.completedFuture(0L);
        }

        CompletableFuture<Long> ioFuture = StorageUtil.supply(ioExecutor, () -> {
            StorageUtil.requireOpen(file);
            prepareFileSync(file);
            return fsTransfer.get();
        });
        registerInFlight(ioKey, ioFuture);
        return ioFuture;
    }


    // ---- AsyncSegmentFile ----

    @Override
    public CompletableFuture<AsyncSegmentFile> open(String path, String prefix, List<String> indexPrefixes, boolean write, String tenant) {
        return open(path, prefix, indexPrefixes, write, tenant, null);
    }

    public CompletableFuture<AsyncSegmentFile> open(String path, String prefix, List<String> indexPrefixes, boolean write, String tenant, CacheMode cacheMode) {
        String key = StorageUtil.segmentKey(path, prefix);
        String ioKey = write ? key : allocateReaderIoKey(key);
        BackingFsMode fsMode = backingFsMode;
        if (fsMode != BackingFsMode.NO_FS) {
            // wait on the canonical writer key before delegate opens channels and initializes cache
            awaitInFlightIo(key, path, false);
        }
        return CompletableFuture.completedFuture(
                openSegmentSync(
                        path, prefix, key, ioKey, indexPrefixes, write, tenant, cacheMode, fsMode));
    }


    private AsyncSegmentFile openSegmentSync(String path, String prefix, String key, String ioKey,
            List<String> indexPrefixes, boolean write, String tenant, CacheMode cacheModeOverride,
            BackingFsMode fsMode) {
        final boolean noFs = fsMode == BackingFsMode.NO_FS;
        CacheMode cacheMode = resolveSegmentCacheMode(cacheModeOverride);
        if (noFs && cacheMode == CacheMode.NO_CACHE) {
            throw new IllegalArgumentException("NO_CACHE is not supported when backing FS mode is NO_FS");
        }
        AsyncSegmentFile file = delegate.openSync(
                path, prefix, key, ioKey, indexPrefixes, write, tenant, noFs);
        delegate.openWithFileEntry(file, noFs, this::registerInFlight, this::scheduleCloseChannels,
                restoreWaitTimeoutMs, ioWaitTimeoutMs);
        file.cacheMode = cacheMode;
        boolean first = false;
        if (cacheMode != CacheMode.NO_CACHE) {
            SegmentFileCacheEntry entry;
            try {
                synchronized (lockFor(key)) {
                    entry = segmentCacheEntries.computeIfAbsent(
                            key, k -> new SegmentFileCacheEntry(memoryTracker));
                    first = entry.retainEntry(write);
                }
            } catch (Throwable t) {
                logger.error("acquire segment cache entry failed for {}, closing file", file.path, t);
                cleanupOpenFailed(file);
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
        }
        try {
            if (cacheMode != CacheMode.NO_CACHE) {
                final boolean noCache = fsMode == BackingFsMode.NO_CACHE;
                initSegmentCache(file, first, noCache, noFs);
                // Initialize index file caches after channels are opened.
                for (AsyncIndexFile af : file.currentIndexFiles.values()) {
                    if (af.cacheEntry != null) {
                        initFileCache(af, af.firstOpener, noCache, noFs);
                    }
                }
            }
        } catch (Throwable t) {
            logger.error("init segment cache failed for {}, closing file", file.path, t);
            cleanupOpenFailed(file);
            throw t;
        }
        return file;
    }

    // Exclusive logical end offset of the segment file (firstOffset + size); 0 if empty.
    private long segmentExclusiveEndOffset(AsyncSegmentFile file) {
        SegmentDirState state = delegate.getSegmentDirState(file);
        return state.isEmpty() ? 0L : state.firstOffset + delegate.sizeSync(file);
    }

    @Override
    public CompletableFuture<Void> position(AsyncSegmentFile file, long offset) {
        if (!file.canRead()) {
            throw new IllegalArgumentException("position() is not supported in write mode");
        }
        StorageUtil.requireOpen(file);
        if (backingFsMode != BackingFsMode.NO_FS) {
            awaitInFlightIo(file.ioKey, file.path, false);
        }
        scheduleCloseChannels(file.path, delegate.positionSync(file, offset));
        return CompletableFuture.completedFuture(null);
    }

    private void scheduleCloseChannels(String path, List<FileChannel> channels) {
        if (channels == null || channels.isEmpty()) {
            return;
        }
        final List<FileChannel> toClose = channels;
        Runnable closeTask = () -> StorageUtil.closeChannels(toClose);
        CompletableFuture<Void> closeFuture = StorageUtil.run(ioExecutor, closeTask);
        if (closeFuture.isCompletedExceptionally()) {
            try {
                closeFuture.join();
            } catch (CompletionException e) {
                if (e.getCause() instanceof RejectedExecutionException) {
                    logger.warn("io executor rejected channel close for {}, fallback to close executor", path);
                    StorageUtil.run(closeExecutor, closeTask);
                }
            }
        }
    }

    private boolean preReadMetadata(AsyncSegmentFile file, long offset, SegmentDirState s, boolean strict) {
        if (!strict && file.isSegmentReady(offset)) {
            return true;
        }
        if (s == null) {
            s = delegate.getSegmentDirState(file);
        }
        AsyncSegmentFile.requireOffsetNotBeforeFirst(s, offset);
        if (strict && file.openedSegmentMatchesState(s, offset)) {
            return true;
        }
        List<FileChannel> pending = new ArrayList<>();
        // switchToSegment never throws, so the pending list is always reachable here.
        boolean ready = file.switchToSegment(offset, s, pending);
        scheduleCloseChannels(file.path, pending);
        return ready;
    }

    /**
     * Allocate a unique reader IO key for inFlight serialization.
     * Format: fileKey + "#r" + stripe-local incrementing number.
     * Different readers get different keys so they don't block each other.
     */
    String allocateReaderIoKey(String fileKey) {
        int stripe = (fileKey.hashCode() & 0x7fffffff) % READER_ID_STRIPES;
        long id = readerIdCounters[stripe].getAndIncrement();
        return fileKey + "#r" + id;
    }


    @Override
    public CompletableFuture<ByteBuf> read(AsyncSegmentFile file, long length) {
        long readOffset = file.position;
        return readInternal(file, length, 0, true,
                () -> preReadMetadata(file, readOffset, null, false),
                () -> executeWithIoFailureHandling(file, () ->
                    delegate.readSync(file, length, readOffset)));
    }

    @Override
    public CompletableFuture<ByteBuf> read(AsyncSegmentFile file, long length, long offset) {
        return readInternal(file, length, offset, false,
                () -> preReadMetadata(file, offset, null, false),
                () -> executeWithIoFailureHandling(file, () ->
                    delegate.readSync(file, length, offset)));
    }


    @Override
    public CompletableFuture<Long> write(AsyncSegmentFile file, ByteBuf data) {
        if (!file.canWrite()) {
            data.release();
            throw new IllegalArgumentException("operation requires write mode: " + file.path);
        }
        if (file.closed) {
            data.release();
            throw new IllegalStateException("file is closed: " + file.path);
        }
        try {
            file.throwIfNoSpace();
        } catch (RuntimeException e) {
            data.release();
            throw e;
        }
        final boolean noFs = backingFsMode == BackingFsMode.NO_FS;
        // No bootstrap roll needed: a writer's tail segment exists from open onwards.
        return writeInternal(file, data, backingFsMode,
                () -> initCacheAndAppend(file, data, noFs),
                writeBuf -> executeWithIoFailureHandling(file, () -> delegate.writeSync(file, writeBuf)),
                () -> executeWithIoFailureHandling(file, () -> {
                    delegate.fsyncSync(file);
                    return null;
                }),
                () -> restoreBackingFsAndAwait(file));
    }

    @Override
    public CompletableFuture<Void> roll(AsyncSegmentFile file) {
        StorageUtil.requireWriteMode(file);
        StorageUtil.requireOpen(file);
        final String id = file.ioKey;
        final BackingFsMode fsMode = backingFsMode;
        final boolean noFs = fsMode == BackingFsMode.NO_FS;
        final boolean noCache = fsMode == BackingFsMode.NO_CACHE;
        if (!noFs) {
            awaitInFlightIo(id, file.path, false);
            file.throwIfNoSpace();
        }

        FileCacheEntry cacheEntry = file.getCacheEntry();

        // 1. noFs but cache not initialized → error.
        if (noFs && (cacheEntry == null || !cacheEntry.isInitialized())) {
            throw new CannotWriteWithoutCacheInNoFsException(
                    file.path, "file cache is not initialized");
        }

        final boolean fsInconsistent = cacheEntry != null && cacheEntry.fsInconsistent;
        final boolean metadataOnly = noFs || fsInconsistent;

        if (!metadataOnly) {
            segmentFlushPendingWriteAndAwait(file, true);
        }

        // 3. Compute segment size.
        final long size;
        if (delegate.getSegmentDirState(file).isEmpty()) {
            size = 0L;
        } else if (cacheEntry != null && cacheEntry.isInitialized()) {
            size = Math.max(0L, cacheEntry.cacheEndOffset - file.openedSegmentStartOffset);
        } else {
            // No cache: get size via IO.
            size = awaitIoCachePrep(file, null, ioWaitTimeoutMs,
                    () -> delegate.sizeOfSegmentSync(file, file.openedSegmentStartOffset), null);
        }

        if (noFs) {
            synchronized (cacheEntry) {
                cacheEntry.fsInconsistent = true;
            }
        }

        List<FileChannel> oldChannels = delegate.rollMetadataSync(file, size, metadataOnly);
        scheduleCloseChannels(file.path, oldChannels);

        initIndexFileCaches(file.currentIndexFiles, true, false, noFs);

        if (noFs) {
            return CompletableFuture.completedFuture(null);
        }

        if (!restoreBackingFsAndAwait(file)) {
            throw new OperationNotExecutedException(file.path);
        }

        // IO phase (ioExecutor): open new segment + index channels, then init their caches.
        CompletableFuture<Void> ioFuture = StorageUtil.run(ioExecutor, () -> {
            StorageUtil.requireOpen(file);
            delegate.initCurrentChannelsSync(file);
            initIndexFileCaches(file.currentIndexFiles, false, noCache, false);
        });
        registerInFlight(id, ioFuture);
        return ioFuture;
    }


    @Override
    public List<Long> list(AsyncSegmentFile file) {
        long[] offsets = delegate.getSegmentDirState(file).offsets();
        List<Long> result = new ArrayList<>(offsets.length);
        for (long offset : offsets) {
            result.add(offset);
        }
        return Collections.unmodifiableList(result);
    }

    @Override
    public long getCurrentSegmentStartOffset(AsyncSegmentFile file) {
        return delegate.getCurrentSegmentStartOffset(file);
    }

    @Override
    public long getStartOffsetByReadOffset(AsyncSegmentFile file, long readOffset) {
        return delegate.getStartOffsetByReadOffset(file, readOffset);
    }

    @Override
    public CompletableFuture<Pair<Long, Map<String, AsyncFile>>> getCurrentIndexFiles(AsyncSegmentFile file, List<String> indexPrefixes) {
        StorageUtil.requireOpen(file);
        final BackingFsMode fsMode = backingFsMode;
        final boolean noFs = fsMode == BackingFsMode.NO_FS;
        final boolean noCache = fsMode == BackingFsMode.NO_CACHE;

        final SegmentDirState s = delegate.getSegmentDirState(file);
        if (s.isEmpty()) {
            return CompletableFuture.completedFuture(Pair.from(0L, new HashMap<>()));
        }

        if (noFs && file.canWrite()) {
            // A writer's index handles are meant to be written to, and under noFs their content can
            // only ever live in the cache. Readers are left to fail in the read path instead, where
            // CannotReadPositionInNoFsException is the fitting error.
            SegmentFileCacheEntry cacheEntry = file.getCacheEntry();
            if (cacheEntry == null || !cacheEntry.isInitialized()) {
                throw new CannotWriteWithoutCacheInNoFsException(file.path, "file cache is not initialized");
            }
        }

        if (!noFs) {
            awaitInFlightIo(file.ioKey, file.path, false);
        }
        if (!file.canWrite()) {
            // Strict: the handles returned must belong to the segment state says holds position.
            // Cannot report empty here — the state was already checked non-empty above.
            preReadMetadata(file, file.position, s, true);
        } else if (!noFs && !restoreBackingFsAndAwait(file)) {
            throw new OperationNotExecutedException(file.path);
        }

        final Pair<Long, Map<String, AsyncIndexFile>> result =
                delegate.getCurrentIndexFilesSync(file, indexPrefixes, noFs);

        if (noFs) {
            initIndexFileCaches(result.getValue(), true, false, true);
            return CompletableFuture.completedFuture(toPublicIndexFiles(result));
        }

        CompletableFuture<Pair<Long, Map<String, AsyncFile>>> ioFuture = StorageUtil.supply(ioExecutor, () -> {
            StorageUtil.requireOpen(file);
            try {
                file.initIndexChannels(result.getValue().values());
            } catch (IOException e) {
                throw StorageUtil.wrapIOException(e);
            }
            initIndexFileCaches(result.getValue(), false, noCache, false);
            return toPublicIndexFiles(result);
        });
        registerInFlight(file.ioKey, ioFuture);
        return ioFuture;
    }

    private Pair<Long, Map<String, AsyncFile>> toPublicIndexFiles(
            Pair<Long, Map<String, AsyncIndexFile>> indexFiles) {
        return Pair.from(indexFiles.getKey(), new HashMap<>(indexFiles.getValue()));
    }

    private void initIndexFileCaches(Map<String, AsyncIndexFile> indexFiles,
            boolean onlyNeedPrepare, boolean noCache, boolean noFs) {
        for (AsyncIndexFile af : indexFiles.values()) {
            if (af.getCacheEntry() == null || (onlyNeedPrepare && !af.needPrepare)) {
                continue;
            }
            initFileCache(af, af.firstOpener, noCache, noFs);
        }
    }


    @Override
    public CompletableFuture<Pair<Long, Map<String, AsyncFile>>> getCurrentIndexFiles(AsyncSegmentFile file) {
        return getCurrentIndexFiles(file, file.indexPrefixes);
    }


    @Override
    public CompletableFuture<Long> size(AsyncSegmentFile file) {
        StorageUtil.requireOpen(file);
        final boolean noFs = backingFsMode == BackingFsMode.NO_FS;
        if (file.cacheMode != CacheMode.NO_CACHE) {
            SegmentFileCacheEntry entry = file.getCacheEntry();
            SegmentDirState state = delegate.getSegmentDirState(file);
            if (state.isEmpty()) {
                return CompletableFuture.completedFuture(0L);
            }
            long firstOffset = state.firstOffset;
            synchronized (entry) {
                if (entry.isInitialized()) {
                    long end = Math.max(entry.writtenToFsOffset, entry.cacheEndOffset);
                    return CompletableFuture.completedFuture(Math.max(0L, end - firstOffset));
                }
            }
        }
        if (noFs) {
            throw new CannotDetermineInNoFsException("size(" + file.path + ")");
        }
        return StorageUtil.supply(ioExecutor, () -> {
            StorageUtil.requireOpen(file);
            return executeWithIoFailureHandling(file, () -> delegate.sizeSync(file));
        });
    }


    @Override
    public CompletableFuture<Long> sizeOfSegment(AsyncSegmentFile file, long startOffset) {
        StorageUtil.requireOpen(file);
        final boolean noFs = backingFsMode == BackingFsMode.NO_FS;
        if (file.cacheMode != CacheMode.NO_CACHE) {
            SegmentDirState state = delegate.getSegmentDirState(file);
            if (state.isEmpty()) {
                return CompletableFuture.completedFuture(0L);
            }
            int idx = state.indexOf(startOffset);
            if (idx < 0) {
                return CompletableFuture.completedFuture(0L);
            }
            if (idx + 1 < state.size()) {
                return CompletableFuture.completedFuture(state.get(idx + 1) - startOffset);
            }
            SegmentFileCacheEntry entry = file.getCacheEntry();
            synchronized (entry) {
                if (entry.isInitialized()) {
                    long end = Math.max(entry.writtenToFsOffset, entry.cacheEndOffset);
                    return CompletableFuture.completedFuture(Math.max(0L, end - startOffset));
                }
            }
        }
        if (noFs) {
            throw new CannotDetermineInNoFsException(
                    "sizeOfSegment(" + file.path + ", " + startOffset + ")");
        }
        return StorageUtil.supply(ioExecutor, () -> {
            StorageUtil.requireOpen(file);
            return executeWithIoFailureHandling(file, () -> delegate.sizeOfSegmentSync(file, startOffset));
        });
    }


    @Override
    public CompletableFuture<Long> lastModified(AsyncSegmentFile file) {
        return lastModifiedOf(backingFsMode == BackingFsMode.NO_FS, file, () -> delegate.lastModified(file));
    }

    private CompletableFuture<Long> lastModifiedOf(boolean noFs, AbstractStorageFile file,
            java.util.function.Supplier<CompletableFuture<Long>> fsLastModified) {
        if (noFs && file.lastModified == 0) {
            file.lastModified = System.currentTimeMillis();
        }
        if (file.lastModified > 0) {
            return CompletableFuture.completedFuture(file.lastModified);
        }
        return fsLastModified.get().thenApply(fsValue -> {
            if (fsValue > file.lastModified) {
                file.lastModified = fsValue;
            }
            return file.lastModified;
        });
    }

    @Override
    public CompletableFuture<Long> lastModifiedOfSegment(AsyncSegmentFile file, long startOffset) {
        if (backingFsMode == BackingFsMode.NO_FS) {
            SegmentDirState state = delegate.getSegmentDirState(file);
            if (!state.isEmpty() && state.lastOffset == startOffset) {
                return lastModified(file);
            }
            throw new CannotDetermineInNoFsException(
                    "lastModifiedOfSegment(" + file.path + ", " + startOffset + ")");
        }
        return delegate.lastModifiedOfSegment(file, startOffset);
    }

    @Override
    public CompletableFuture<Void> deleteSegments(AsyncSegmentFile file, List<Long> startOffsets) {
        StorageUtil.requireWriteMode(file);
        StorageUtil.requireOpen(file);
        final String id = file.ioKey;

        if (startOffsets.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        SegmentFileCacheEntry cacheEntry = file.getCacheEntry();
        final boolean noFs = backingFsMode == BackingFsMode.NO_FS;

        if (noFs && (cacheEntry == null || !cacheEntry.isInitialized())) {
            throw new CannotWriteWithoutCacheInNoFsException(file.path, "file cache is not initialized");
        }
        if (!noFs) {
            awaitInFlightIo(id, file.path, false);
            file.throwIfNoSpace();
        }

        SegmentDirState state = delegate.getSegmentDirState(file);
        if (state.isEmpty()) {
            throw new IllegalArgumentException("deleteSegments on a segment file with no segments: " + file.path);
        }
        long lastDeletedOffset = startOffsets.get(startOffsets.size() - 1);
        long firstOffset = state.firstOffset;

        boolean alreadyDropped = lastDeletedOffset < firstOffset;

        if (alreadyDropped) {
            for (long startOffset : startOffsets) {
                if (startOffset > lastDeletedOffset) {
                    throw new IllegalArgumentException(
                            "deleteSegments requires the last requested offset to be the greatest: "
                                    + lastDeletedOffset + " < " + startOffset);
                }
            }
        } else {
            int drop = startOffsets.size();
            if (drop >= state.size()) {
                throw new IllegalArgumentException("deleteSegments cannot delete the last segment");
            }
            for (int i = 0; i < drop; i++) {
                if (startOffsets.get(i) != state.get(i)) {
                    throw new IllegalArgumentException(
                            "deleteSegments requires deleting segments in order from the first: expected "
                                    + state.get(i) + ", got " + startOffsets.get(i));
                }
            }
        }

        long[] droppedOffsets = SegmentDirState.EMPTY.offsets();
        if (!alreadyDropped) {
            droppedOffsets = delegate.deleteSegmentsMetadataSync(file, lastDeletedOffset);
            long newFirstOffset = delegate.getSegmentDirState(file).firstOffset;
            if (file.cacheMode != CacheMode.NO_CACHE) {
                synchronized (cacheEntry) {
                    if (cacheEntry.isInitialized()) {
                        long cacheStart = cacheEntry.cacheStartOffset;
                        if (cacheStart < newFirstOffset) {
                            cacheEntry.dropCacheBefore(newFirstOffset, chunkSize);
                        }
                        if (noFs) {
                            // The dropped segments left metadata but their files are still on disk.
                            cacheEntry.fsInconsistent = true;
                        }
                    }
                }
            }
            file.lastModified = System.currentTimeMillis();
        }

        if (noFs) {
            return CompletableFuture.completedFuture(null);
        }

        if (!restoreBackingFsAndAwait(file)) {
            throw new OperationNotExecutedException(file.path);
        }

        if (!file.mayHaveOrphanFiles) {
            return CompletableFuture.completedFuture(null);
        }

        final long[] toUnlink = droppedOffsets;
        CompletableFuture<Void> ioFuture = StorageUtil.run(ioExecutor, () -> {
            StorageUtil.requireOpen(file);
            delegate.deleteSegmentsIo(file, toUnlink);
        });
        registerInFlight(id, ioFuture);
        return ioFuture;
    }

    @Override
    public CompletableFuture<Void> delete(AsyncSegmentFile file) {
        StorageUtil.requireWriteMode(file);
        StorageUtil.requireOpen(file);
        final String id = file.ioKey;

        final SegmentFileCacheEntry cacheEntry = file.getCacheEntry();
        final boolean noFs = backingFsMode == BackingFsMode.NO_FS;

        if (noFs && (cacheEntry == null || !cacheEntry.isInitialized())) {
            throw new CannotWriteWithoutCacheInNoFsException(file.path, "file cache is not initialized");
        }
        if (!noFs) {
            awaitInFlightIo(id, file.path, false);
            file.throwIfNoSpace();
        }

        SegmentDirState state = delegate.getSegmentDirState(file);
        long[] droppedOffsets = state.offsets();
        final boolean alreadyDeleted = state.isEmpty();

        if (!alreadyDeleted) {
            if (file.cacheMode != CacheMode.NO_CACHE) {
                synchronized (cacheEntry) {
                    if (cacheEntry.isInitialized()) {
                        cacheEntry.clear();
                        if (noFs) {
                            cacheEntry.fsInconsistent = true;
                        }
                    }
                }
            }

            List<FileChannel> oldChannels = delegate.deleteMetadataSync(file);
            scheduleCloseChannels(file.path, oldChannels);
        }

        if (noFs) {
            return CompletableFuture.completedFuture(null);
        }

        if (!restoreBackingFsAndAwait(file)) {
            throw new OperationNotExecutedException(file.path);
        }

        if (!file.mayHaveOrphanFiles) {
            return CompletableFuture.completedFuture(null);
        }

        final long[] toUnlink = droppedOffsets;
        CompletableFuture<Void> ioFuture = StorageUtil.run(ioExecutor, () -> {
            StorageUtil.requireOpen(file);
            delegate.deleteSegmentsIo(file, toUnlink);
        });
        registerInFlight(id, ioFuture);
        return ioFuture;
    }

    @Override
    public CompletableFuture<Void> truncate(AsyncSegmentFile file, long offset) {
        StorageUtil.requireWriteMode(file);
        StorageUtil.requireOpen(file);
        final String id = file.ioKey;
        final boolean noFs = backingFsMode == BackingFsMode.NO_FS;

        SegmentFileCacheEntry entry = file.getCacheEntry();
        if (noFs && (entry == null || !entry.isInitialized())) {
            throw new CannotWriteWithoutCacheInNoFsException(file.path, "file cache is not initialized");
        }
        if (!noFs) {
            awaitInFlightIo(id, file.path, false);
            file.throwIfNoSpace();
        }

        Long endOffset = null;
        if (entry != null) {
            synchronized (entry) {
                if (entry.isInitialized()) {
                    endOffset = entry.cacheEndOffset;
                    SegmentDirState state = delegate.getSegmentDirState(file);
                    long firstStart = state.isEmpty() ? 0L : state.firstOffset;
                    entry.truncateTo(offset, chunkSize, firstStart);
                    if (noFs) {
                        entry.fsInconsistent = true;
                    }
                }
            }
        }
        file.lastModified = System.currentTimeMillis();
        final long truncateEndOffset = endOffset != null
                ? endOffset
                : awaitIoCachePrep(file, null, ioWaitTimeoutMs,
                        () -> segmentExclusiveEndOffset(file), null);

        // Metadata phase (main thread).
        List<FileChannel> oldChannels = new ArrayList<>();
        final long[] droppedOffsets =
                delegate.truncateSync(file, offset, truncateEndOffset, noFs, oldChannels);
        scheduleCloseChannels(file.path, oldChannels);

        if (noFs) {
            return CompletableFuture.completedFuture(null);
        }

        if (!restoreBackingFsAndAwait(file)) {
            throw new OperationNotExecutedException(file.path);
        }

        CompletableFuture<Void> ioFuture = StorageUtil.supply(ioExecutor, () -> {
            StorageUtil.requireOpen(file);
            delegate.initCurrentChannelsSync(file);
            executeWithIoFailureHandling(file, () -> {
                delegate.truncateLastSegmentChannel(file, offset);
                return null;
            });
            delegate.deleteSegmentsIo(file, droppedOffsets);
            FileCacheEntry cacheEntry = file.getCacheEntry();
            if (cacheEntry != null) {
                synchronized (cacheEntry) {
                    if (!cacheEntry.fsInconsistent) {
                        cacheEntry.pendingFsyncBytes = file.pendingFsyncBytes;
                    }
                }
            }
            return null;
        });
        registerInFlight(id, ioFuture);
        return ioFuture;
    }


    @Override
    public CompletableFuture<Void> close(AsyncSegmentFile file) {
        for (AsyncIndexFile indexFile : file.currentIndexFiles.values()) {
            if (indexFile.noSpaceFailure != null) {
                file.markNoSpace(indexFile.noSpaceFailure);
                break;
            }
        }
        return closeInternal(file, () -> delegate.closeSync(file), f -> segmentFlushPendingWriteAndAwait(f, false),
                () -> restoreBackingFsAndAwait(file));
    }

    @Override
    public CompletableFuture<Void> fsync(AsyncSegmentFile file) {
        return fsyncInternal(file,
                () -> flushPendingWriteAndAwait(file,
                        writeBuf -> executeWithIoFailureHandling(file,
                                () -> delegate.writeSync(file, writeBuf)),
                        () -> executeWithIoFailureHandling(file, () -> {
                            delegate.fsyncSync(file);
                            return null;
                        }),
                        false),
                () -> executeWithIoFailureHandling(file, () -> {
                    delegate.fsyncSync(file);
                    return null;
                }),
                () -> restoreBackingFsAndAwait(file));
    }

    @Override
    public CompletableFuture<Long> transferTo(AsyncSegmentFile file, long offset, long count, WritableByteChannel target) {
        return transferToInternal(file, offset, count, target,
                () -> preReadMetadata(file, offset, null, false),
                () -> executeWithIoFailureHandling(file, () ->
                    delegate.transferToSync(file, offset, count, target)));
    }

}
