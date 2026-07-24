package com.ctrip.xpipe.redis.keeper.storage;

public class StorageIOException extends RuntimeException {

    public StorageIOException(Throwable cause) {
        super(cause);
    }

    public StorageIOException(String message) {
        super(message);
    }

    public StorageIOException(String message, Throwable cause) {
        super(message, cause);
    }
}