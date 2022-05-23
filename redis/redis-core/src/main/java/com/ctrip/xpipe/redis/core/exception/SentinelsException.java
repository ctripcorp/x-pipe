package com.ctrip.xpipe.redis.core.exception;

public class SentinelsException extends RedisException {

    public SentinelsException(String message) {
        super(message);
    }
    public SentinelsException(String message, Throwable th) {
        super(message, th);
    }
}
