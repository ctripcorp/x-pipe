package com.ctrip.xpipe.redis.console.exception;

public class NotEnoughResultsException extends RedisConsoleRuntimeException {


    public NotEnoughResultsException(String message) {
        super(message);
    }

    public NotEnoughResultsException(String message, Throwable th) {
        super(message, th);
    }

}
