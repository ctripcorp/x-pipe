package com.ctrip.xpipe.redis.keeper.storage;

// Thrown when FULL_CACHE preload fails during write before data is cached.
public class PreloadFailedException extends RuntimeException {

    public PreloadFailedException(String message) {
        super(message);
    }
}
