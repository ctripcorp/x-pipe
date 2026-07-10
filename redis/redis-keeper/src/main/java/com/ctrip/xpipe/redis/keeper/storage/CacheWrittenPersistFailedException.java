package com.ctrip.xpipe.redis.keeper.storage;

// Thrown when data is already in the in-memory cache but persisting to the backing
// store failed. Callers must not retry write with the same buffer; reconcile via
// a flush-only path instead.
public class CacheWrittenPersistFailedException extends RuntimeException {

    public CacheWrittenPersistFailedException(Throwable cause) {
        super(cause);
    }

    public CacheWrittenPersistFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
