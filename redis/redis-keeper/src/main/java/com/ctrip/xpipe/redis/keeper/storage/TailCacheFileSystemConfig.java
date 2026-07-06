package com.ctrip.xpipe.redis.keeper.storage;

import com.ctrip.xpipe.redis.keeper.storage.AbstractStorageFile.CacheMode;

public class TailCacheFileSystemConfig {

    public enum BackingFsMode {
        // Write to FS before returning; write failure surfaces as an error immediately.
        SYNC,
        // Write to cache and return immediately; data is flushed to FS asynchronously.
        // Required when batching writes to reduce FS IO.
        ASYNC,
        // All reads and writes go through cache only; FS is never touched.
        // Unlike SYNC/ASYNC, data may be lost before fsync ensures durability.
        // Intended for use when FS is unavailable, effectively making the system memory-only.
        // When FS is restored, any file/segment fully retained in cache is guaranteed to be flushed.
        DISABLED
    }

    // Setting to false prefers reading from the backing FS when data is available there; only intended for testing.
    private boolean readPreferCache = true;
    // Cache read requires a memory-to-kernel copy; transferTo is zero-copy when page cache is hot,
    // but a page cache miss blocks the calling thread on disk IO. Cache hit and page cache hit are
    // independent — cache hit with page cache miss is entirely possible. Defaults to true for
    // more stable behavior.
    private boolean transferPreferCache = true;
    private BackingFsMode backingFsMode = BackingFsMode.SYNC;
    private long maxCacheSizeBytes = 0;
    private long maxCacheSizePerTenantBytes = 0;
    private long expectedMinRetentionMs = 0;
    // Dynamically switching defaultCacheMode to FULL_CACHE does not backfill existing cache entries
    // if the writer is already open with a different cache mode.
    // only data written after the switch is cached.
    private CacheMode defaultCacheMode = CacheMode.NO_CACHE;

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

    public CacheMode getDefaultCacheMode() {
        return defaultCacheMode;
    }

    public TailCacheFileSystemConfig setDefaultCacheMode(CacheMode defaultCacheMode) {
        if (defaultCacheMode == CacheMode.DYNAMIC) {
            throw new IllegalArgumentException("DYNAMIC cannot be used as defaultCacheMode");
        }
        this.defaultCacheMode = defaultCacheMode;
        return this;
    }
}
