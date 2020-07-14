package com.ctrip.xpipe.redis.meta.server.exception;

import com.ctrip.xpipe.redis.core.exception.RedisException;

public class BadRedisVersionException extends RedisException {

    public BadRedisVersionException(String message) {
        super(message);
    }

    public BadRedisVersionException(String message, Throwable th){
        super(message, th);
    }

}
