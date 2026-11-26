package com.ctrip.xpipe.redis.keeper.container.resp;

import com.ctrip.xpipe.redis.keeper.storage.TailCacheMemoryStats;

public class TailCacheMemoryResp {

    private final boolean initialized;
    private final long snapshotTimeMillis;
    private final long maxCacheSizeBytes;
    private final long committedBytes;
    private final long unDurableBytes;

    private TailCacheMemoryResp(boolean initialized, long snapshotTimeMillis, long maxCacheSizeBytes,
                                long committedBytes, long unDurableBytes) {
        this.initialized = initialized;
        this.snapshotTimeMillis = snapshotTimeMillis;
        this.maxCacheSizeBytes = maxCacheSizeBytes;
        this.committedBytes = committedBytes;
        this.unDurableBytes = unDurableBytes;
    }

    public static TailCacheMemoryResp from(TailCacheMemoryStats stats) {
        return new TailCacheMemoryResp(true, stats.getSnapshotTimeMillis(), stats.getMaxCacheSizeBytes(),
                stats.getCommittedBytes(), stats.getUnDurableBytes());
    }

    public static TailCacheMemoryResp notInitialized(long nowMillis) {
        return new TailCacheMemoryResp(false, nowMillis, 0, 0, 0);
    }

    public boolean isInitialized() {
        return initialized;
    }

    public long getSnapshotTimeMillis() {
        return snapshotTimeMillis;
    }

    public long getMaxCacheSizeBytes() {
        return maxCacheSizeBytes;
    }

    public long getCommittedBytes() {
        return committedBytes;
    }

    public long getUnDurableBytes() {
        return unDurableBytes;
    }
}
