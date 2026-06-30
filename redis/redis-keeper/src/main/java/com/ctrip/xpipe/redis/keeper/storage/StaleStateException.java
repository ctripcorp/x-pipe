package com.ctrip.xpipe.redis.keeper.storage;

// Thrown when the in-memory view diverges from the underlying filesystem state
// (e.g. a path was deleted out-of-band, a CREATE_NEW path already exists, or a
// channel was closed under us). Callers should rebuild their view (reopen,
// recompute offsets/args) and retry; plain retry on the same operation will not
// recover.
public class StaleStateException extends RuntimeException {
    public StaleStateException(Throwable cause) { super(cause); }
    public StaleStateException(String message) { super(message); }
    public StaleStateException(String message, Throwable cause) { super(message, cause); }
}
