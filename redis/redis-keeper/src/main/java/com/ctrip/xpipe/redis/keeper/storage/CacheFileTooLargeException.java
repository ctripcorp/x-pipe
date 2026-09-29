package com.ctrip.xpipe.redis.keeper.storage;

public class CacheFileTooLargeException extends RuntimeException {

    public CacheFileTooLargeException(String path, long size) {
        super("cache file too large: path=" + path + ", size=" + size);
    }
}
