package com.ctrip.xpipe.redis.keeper.storage;

// Thrown when an operation cannot actually execute.
public class OperationNotExecutedException extends RuntimeException {

    public OperationNotExecutedException(String identifier) {
        super("operation was not executed for: " + identifier);
    }

    public OperationNotExecutedException(String identifier, Throwable cause) {
        super("operation was not executed for: " + identifier, cause);
    }
}
