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
    // but a page cache miss blocks the calling thread on disk IO. Cache hit and page cache hit are
    // independent — cache hit with page cache miss is entirely possible. Defaults to true for
    // more stable behavior.
    private boolean transferPreferCache = true;
    private BackingFsMode backingFsMode = BackingFsMode.ASYNC;
    private long maxCacheSizeBytes = 0;
    private long maxCacheSizePerTenantBytes = 0;
    private long expectedMinRetentionMs = 0;
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
    private int eioRetryMaxAttempts = 3;

    public TailCacheFileSystemConfig() {
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
        this.maxCacheSizeBytes = maxCacheSizeBytes;
        return this;
    }

    public long getMaxCacheSizePerTenantBytes() {
        return maxCacheSizePerTenantBytes;
    }

    public TailCacheFileSystemConfig setMaxCacheSizePerTenantBytes(long maxCacheSizePerTenantBytes) {
        this.maxCacheSizePerTenantBytes = maxCacheSizePerTenantBytes;
        return this;
    }

    public long getExpectedMinRetentionMs() {
        return expectedMinRetentionMs;
    }

    public TailCacheFileSystemConfig setExpectedMinRetentionMs(long expectedMinRetentionMs) {
        this.expectedMinRetentionMs = expectedMinRetentionMs;
        return this;
    }

    public long getChunkSize() {
        return chunkSize;
    }

    public TailCacheFileSystemConfig setChunkSize(long chunkSize) {
        if (chunkSize <= 0) throw new IllegalArgumentException("chunkSize must be positive");
        this.chunkSize = chunkSize;
        return this;
    }

    public int getPreloadChunkThreshold() {
        return preloadChunkThreshold;
    }

    public TailCacheFileSystemConfig setPreloadChunkThreshold(int preloadChunkThreshold) {
        if (preloadChunkThreshold <= 0) throw new IllegalArgumentException("preloadChunkThreshold must be positive");
        this.preloadChunkThreshold = preloadChunkThreshold;
        return this;
    }

    public long getPreloadTimeoutMs() {
        return preloadTimeoutMs;
    }

    public TailCacheFileSystemConfig setPreloadTimeoutMs(long preloadTimeoutMs) {
        if (preloadTimeoutMs < 0) throw new IllegalArgumentException("preloadTimeoutMs must be non-negative");
        this.preloadTimeoutMs = preloadTimeoutMs;
        return this;
    }

    public long getIoWaitTimeoutMs() {
        return ioWaitTimeoutMs;
    }

    public TailCacheFileSystemConfig setIoWaitTimeoutMs(long ioWaitTimeoutMs) {
        if (ioWaitTimeoutMs < 0) throw new IllegalArgumentException("ioWaitTimeoutMs must be non-negative");
        this.ioWaitTimeoutMs = ioWaitTimeoutMs;
        return this;
    }

    public long getWriteBatchBytes() {
        return writeBatchBytes;
    }

    public TailCacheFileSystemConfig setWriteBatchBytes(long writeBatchBytes) {
        if (writeBatchBytes <= 0) throw new IllegalArgumentException("writeBatchBytes must be positive");
        this.writeBatchBytes = writeBatchBytes;
        return this;
    }

    public int getMaxWriteChunkThreshold() {
        return maxWriteChunkThreshold;
    }

    public TailCacheFileSystemConfig setMaxWriteChunkThreshold(int maxWriteChunkThreshold) {
        if (maxWriteChunkThreshold <= 0) throw new IllegalArgumentException("maxWriteChunkThreshold must be positive");
        this.maxWriteChunkThreshold = maxWriteChunkThreshold;
        return this;
    }

    public int getEioRetryMaxAttempts() {
        return eioRetryMaxAttempts;
    }

    public TailCacheFileSystemConfig setEioRetryMaxAttempts(int eioRetryMaxAttempts) {
        if (eioRetryMaxAttempts <= 0) throw new IllegalArgumentException("eioRetryMaxAttempts must be positive");
        this.eioRetryMaxAttempts = eioRetryMaxAttempts;
        return this;
    }
}
