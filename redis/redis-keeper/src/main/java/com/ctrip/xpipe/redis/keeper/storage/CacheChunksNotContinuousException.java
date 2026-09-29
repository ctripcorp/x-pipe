package com.ctrip.xpipe.redis.keeper.storage;

public class CacheChunksNotContinuousException extends RuntimeException {

    public CacheChunksNotContinuousException(String message) {
        super(message);
    }
}
