package com.ctrip.xpipe.redis.keeper.storage;

public class CacheFileTooLargeException extends RuntimeException {

    public CacheFileTooLargeException(String key, long size) {
        super("cache file too large: key=" + key + ", size=" + size);
    }
}
