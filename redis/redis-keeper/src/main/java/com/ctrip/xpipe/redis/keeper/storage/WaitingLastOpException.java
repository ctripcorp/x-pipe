package com.ctrip.xpipe.redis.keeper.storage;

// Thrown when truncate/close (or NO_CACHE write) cannot proceed while waiting for a prior in-flight IO.
public class WaitingLastOpException extends RuntimeException {

    public WaitingLastOpException(String identifier) {
        super("waiting for prior IO failed on: " + identifier);
    }

    public WaitingLastOpException(String identifier, Throwable cause) {
        super("waiting for prior IO failed on: " + identifier, cause);
    }
}
