package com.ctrip.xpipe.redis.console.service.migration.exception;

import com.ctrip.xpipe.redis.console.exception.RedisConsoleException;

public class NoResponseException extends RedisConsoleException {

    public NoResponseException(String message) {
        super(message);
    }

    public NoResponseException(String msg, Throwable th) {
        super(msg, th);
    }
}
