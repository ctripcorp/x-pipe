package com.ctrip.xpipe.redis.keeper.storage;

/** Approximate, read without locks. */
public final class TailCacheMemoryStats {

    private final long snapshotTimeMillis;
    private final long maxCacheSizeBytes;
    private final long committedBytes;
    private final long unDurableBytes;

    public TailCacheMemoryStats(long snapshotTimeMillis, long maxCacheSizeBytes, long committedBytes,
                                long unDurableBytes) {
        this.snapshotTimeMillis = snapshotTimeMillis;
        this.maxCacheSizeBytes = maxCacheSizeBytes;
        this.committedBytes = committedBytes;
        this.unDurableBytes = unDurableBytes;
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

    @Override
    public String toString() {
        return "TailCacheMemoryStats{snapshotTimeMillis=" + snapshotTimeMillis + ", maxCacheSizeBytes="
                + maxCacheSizeBytes + ", committedBytes=" + committedBytes + ", unDurableBytes=" + unDurableBytes + '}';
    }
}
