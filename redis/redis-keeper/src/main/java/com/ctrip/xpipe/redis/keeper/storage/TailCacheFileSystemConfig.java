package com.ctrip.xpipe.redis.keeper.storage;

public class TailCacheFileSystemConfig {

    public enum BackingFsMode {
        // Cache writes; flush to FS asynchronously and return without waiting for IO.
        ASYNC,
        // Cache reads and writes; FS is never touched.
        NO_FS,
        // No in-memory cache for writes; go directly to FS and wait for IO.
        NO_CACHE
    }

    // Setting to false prefers reading from the backing FS when data is available there; only intended for testing.
    private boolean readPreferCache = true;
    // Cache read requires a memory-to-kernel copy; transferTo is zero-copy when page cache is hot,
    // but a page cache miss blocks the calling thread on disk IO. Cache hit and page cache miss are
    // independent — cache hit with page cache miss is entirely possible. Defaults to true for
    // more stable behavior.
    private boolean transferPreferCache = true;
    private BackingFsMode backingFsMode = BackingFsMode.ASYNC;
    private long maxCacheSizeBytes = 1024 * 1024 * 1024;
    private long maxCacheSizePerFileBytes = 100 * 1024 * 1024;
    private int minRetainChunks = 1;
    private long expectedMinRetentionMs = 0;
    private double lowWatermarkRatio = 0.7;
    private double highWatermarkRatio = 0.9;
    private long evictScanIntervalMs = 60_000;
    private double evictBandWidthRatio = 0.1;
    private int evictBandCount = 3;
    private double maxEvictRatioPerWrite = 0.2;
    private long chunkSize = 1 * 1024 * 1024;
    // When file size <= preloadChunkThreshold * chunkSize, use aligned reads for zero-copy cache population.
    // Otherwise read the whole file in one shot and copy into chunks. Default is 8.
    private int preloadChunkThreshold = 8;
    // Max wait for FULL_CACHE preload; 0 wait until preload completes.
    private long preloadTimeoutMs = 1000;
    // Max wait for IO.
    private long ioWaitTimeoutMs = 1000;
    private long writeBatchBytes = 1 * 1024 * 1024;
    private int maxWriteChunkThreshold = 32;
    private int eioRetryMaxAttempts = 1;

    public TailCacheFileSystemConfig() {
    }

    public static void validateMaxCacheSizeBytes(long maxCacheSizeBytes) {
        if (maxCacheSizeBytes <= 0) throw new IllegalArgumentException("maxCacheSizeBytes must be positive");
    }

    public static void validatePerFileCacheLimits(long maxCacheSizePerFileBytes, int minRetainChunks, long chunkSize) {
        if (maxCacheSizePerFileBytes <= 0) {
            throw new IllegalArgumentException("maxCacheSizePerFileBytes must be positive");
        }
        if (minRetainChunks < 1) {
            throw new IllegalArgumentException("minRetainChunks must be positive");
        }
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("chunkSize must be positive");
        }
        if (minRetainChunks > maxCacheSizePerFileBytes / chunkSize) {
            throw new IllegalArgumentException(
                    "minRetainChunks * chunkSize must not exceed maxCacheSizePerFileBytes");
        }
    }

    public static void validateExpectedMinRetentionMs(long expectedMinRetentionMs) {
        if (expectedMinRetentionMs < 0) {
            throw new IllegalArgumentException("expectedMinRetentionMs must be non-negative");
        }
    }

    public static void validateWatermarkRatios(double lowWatermarkRatio, double highWatermarkRatio) {
        if (!(lowWatermarkRatio > 0 && lowWatermarkRatio < highWatermarkRatio && highWatermarkRatio <= 1)) {
            throw new IllegalArgumentException("require 0 < lowWatermarkRatio < highWatermarkRatio <= 1");
        }
    }

    public static void validateEvictScanIntervalMs(long evictScanIntervalMs) {
        if (evictScanIntervalMs <= 0) throw new IllegalArgumentException("evictScanIntervalMs must be positive");
    }

    public static void validateEvictBands(double evictBandWidthRatio, int evictBandCount) {
        if (!(evictBandWidthRatio > 0 && evictBandCount >= 1
                && evictBandWidthRatio * evictBandCount <= 1)) {
            throw new IllegalArgumentException(
                    "require evictBandWidthRatio > 0, evictBandCount >= 1, and coverage <= 1");
        }
    }

    public static void validatePreloadChunkThreshold(int preloadChunkThreshold) {
        if (preloadChunkThreshold <= 0) throw new IllegalArgumentException("preloadChunkThreshold must be positive");
    }

    public static void validatePreloadTimeoutMs(long preloadTimeoutMs) {
        if (preloadTimeoutMs < 0) throw new IllegalArgumentException("preloadTimeoutMs must be non-negative");
    }

    public static void validateIoWaitTimeoutMs(long ioWaitTimeoutMs) {
        if (ioWaitTimeoutMs < 0) throw new IllegalArgumentException("ioWaitTimeoutMs must be non-negative");
    }

    public static void validateWriteBatchBytes(long writeBatchBytes) {
        if (writeBatchBytes <= 0) throw new IllegalArgumentException("writeBatchBytes must be positive");
    }

    public static void validateMaxWriteChunkThreshold(int maxWriteChunkThreshold) {
        if (maxWriteChunkThreshold <= 0) throw new IllegalArgumentException("maxWriteChunkThreshold must be positive");
    }

    public static void validateEioRetryMaxAttempts(int eioRetryMaxAttempts) {
        if (eioRetryMaxAttempts <= 0) throw new IllegalArgumentException("eioRetryMaxAttempts must be positive");
    }

    public boolean isReadPreferCache() {
        return readPreferCache;
    }

    public TailCacheFileSystemConfig setReadPreferCache(boolean readPreferCache) {
        this.readPreferCache = readPreferCache;
        return this;
    }

    public boolean isTransferPreferCache() {
        return transferPreferCache;
    }

    public TailCacheFileSystemConfig setTransferPreferCache(boolean transferPreferCache) {
        this.transferPreferCache = transferPreferCache;
        return this;
    }

    public BackingFsMode getBackingFsMode() {
        return backingFsMode;
    }

    public TailCacheFileSystemConfig setBackingFsMode(BackingFsMode backingFsMode) {
        this.backingFsMode = backingFsMode;
        return this;
    }

    public long getMaxCacheSizeBytes() {
        return maxCacheSizeBytes;
    }

    public TailCacheFileSystemConfig setMaxCacheSizeBytes(long maxCacheSizeBytes) {
        validateMaxCacheSizeBytes(maxCacheSizeBytes);
        this.maxCacheSizeBytes = maxCacheSizeBytes;
        return this;
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

    public TailCacheFileSystemConfig setPerFileCacheLimits(long maxCacheSizePerFileBytes, int minRetainChunks,
            long chunkSize) {
        validatePerFileCacheLimits(maxCacheSizePerFileBytes, minRetainChunks, chunkSize);
        this.maxCacheSizePerFileBytes = maxCacheSizePerFileBytes;
        this.minRetainChunks = minRetainChunks;
        this.chunkSize = chunkSize;
        return this;
    }

    public long getExpectedMinRetentionMs() {
        return expectedMinRetentionMs;
    }

    public TailCacheFileSystemConfig setExpectedMinRetentionMs(long expectedMinRetentionMs) {
        validateExpectedMinRetentionMs(expectedMinRetentionMs);
        this.expectedMinRetentionMs = expectedMinRetentionMs;
        return this;
    }

    public double getLowWatermarkRatio() {
        return lowWatermarkRatio;
    }

    public double getHighWatermarkRatio() {
        return highWatermarkRatio;
    }

    public TailCacheFileSystemConfig setWatermarkRatios(double lowWatermarkRatio, double highWatermarkRatio) {
        validateWatermarkRatios(lowWatermarkRatio, highWatermarkRatio);
        this.lowWatermarkRatio = lowWatermarkRatio;
        this.highWatermarkRatio = highWatermarkRatio;
        return this;
    }

    public long getEvictScanIntervalMs() {
        return evictScanIntervalMs;
    }

    public TailCacheFileSystemConfig setEvictScanIntervalMs(long evictScanIntervalMs) {
        validateEvictScanIntervalMs(evictScanIntervalMs);
        this.evictScanIntervalMs = evictScanIntervalMs;
        return this;
    }

    public double getEvictBandWidthRatio() {
        return evictBandWidthRatio;
    }

    public int getEvictBandCount() {
        return evictBandCount;
    }

    public TailCacheFileSystemConfig setEvictBands(double evictBandWidthRatio, int evictBandCount) {
        validateEvictBands(evictBandWidthRatio, evictBandCount);
        this.evictBandWidthRatio = evictBandWidthRatio;
        this.evictBandCount = evictBandCount;
        return this;
    }

    public static void validateMaxEvictRatioPerWrite(double maxEvictRatioPerWrite) {
        if (maxEvictRatioPerWrite <= 0 || maxEvictRatioPerWrite > 1) {
            throw new IllegalArgumentException("maxEvictRatioPerWrite must be in (0, 1]");
        }
    }

    public double getMaxEvictRatioPerWrite() {
        return maxEvictRatioPerWrite;
    }

    public TailCacheFileSystemConfig setMaxEvictRatioPerWrite(double maxEvictRatioPerWrite) {
        validateMaxEvictRatioPerWrite(maxEvictRatioPerWrite);
        this.maxEvictRatioPerWrite = maxEvictRatioPerWrite;
        return this;
    }

    public int getPreloadChunkThreshold() {
        return preloadChunkThreshold;
    }

    public TailCacheFileSystemConfig setPreloadChunkThreshold(int preloadChunkThreshold) {
        validatePreloadChunkThreshold(preloadChunkThreshold);
        this.preloadChunkThreshold = preloadChunkThreshold;
        return this;
    }

    public long getPreloadTimeoutMs() {
        return preloadTimeoutMs;
    }

    public TailCacheFileSystemConfig setPreloadTimeoutMs(long preloadTimeoutMs) {
        validatePreloadTimeoutMs(preloadTimeoutMs);
        this.preloadTimeoutMs = preloadTimeoutMs;
        return this;
    }

    public long getIoWaitTimeoutMs() {
        return ioWaitTimeoutMs;
    }

    public TailCacheFileSystemConfig setIoWaitTimeoutMs(long ioWaitTimeoutMs) {
        validateIoWaitTimeoutMs(ioWaitTimeoutMs);
        this.ioWaitTimeoutMs = ioWaitTimeoutMs;
        return this;
    }

    public long getWriteBatchBytes() {
        return writeBatchBytes;
    }

    public TailCacheFileSystemConfig setWriteBatchBytes(long writeBatchBytes) {
        validateWriteBatchBytes(writeBatchBytes);
        this.writeBatchBytes = writeBatchBytes;
        return this;
    }

    public int getMaxWriteChunkThreshold() {
        return maxWriteChunkThreshold;
    }

    public TailCacheFileSystemConfig setMaxWriteChunkThreshold(int maxWriteChunkThreshold) {
        validateMaxWriteChunkThreshold(maxWriteChunkThreshold);
        this.maxWriteChunkThreshold = maxWriteChunkThreshold;
        return this;
    }

    public int getEioRetryMaxAttempts() {
        return eioRetryMaxAttempts;
    }

    public TailCacheFileSystemConfig setEioRetryMaxAttempts(int eioRetryMaxAttempts) {
        validateEioRetryMaxAttempts(eioRetryMaxAttempts);
        this.eioRetryMaxAttempts = eioRetryMaxAttempts;
        return this;
    }
}
