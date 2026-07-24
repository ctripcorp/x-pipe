package com.ctrip.xpipe.redis.keeper.storage;

public class CacheMemoryReserveException extends RuntimeException {

    public CacheMemoryReserveException(long bytes, long limitBytes, long committedBytes) {
        super("cache memory reserve timeout: requested=" + bytes
                + ", limit=" + limitBytes
                + ", committed=" + committedBytes);
    }

    public CacheMemoryReserveException(long bytes, Throwable cause) {
        super("cache memory reserve failed: requested=" + bytes, cause);
    }
}
