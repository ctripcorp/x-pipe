package com.ctrip.xpipe.redis.keeper.storage;

/** Approximate, read without locks. */
public final class TailCacheFileMemoryStats {

    public enum Kind {
        FILE,
        // excludes index entries
        SEGMENT,
        INDEX
    }

    private final Kind kind;
    private final String path;
    private final long cacheSizeBytes;
    private final long unDurableBytes;

    public TailCacheFileMemoryStats(Kind kind, String path, long cacheSizeBytes, long unDurableBytes) {
        this.kind = kind;
        this.path = path;
        this.cacheSizeBytes = cacheSizeBytes;
        this.unDurableBytes = unDurableBytes;
    }

    public Kind getKind() {
        return kind;
    }

    public String getPath() {
        return path;
    }

    public long getCacheSizeBytes() {
        return cacheSizeBytes;
    }

    public long getUnDurableBytes() {
        return unDurableBytes;
    }

    @Override
    public String toString() {
        return "TailCacheFileMemoryStats{kind=" + kind + ", path='" + path + "', cacheSizeBytes=" + cacheSizeBytes
                + ", unDurableBytes=" + unDurableBytes + '}';
    }
}
