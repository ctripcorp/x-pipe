package com.ctrip.xpipe.redis.console.service.migration.exception;

import com.ctrip.xpipe.redis.console.exception.RedisConsoleException;

public class MigrationSystemNotHealthyException extends RedisConsoleException {

    public MigrationSystemNotHealthyException(String message) {
        super(message);
    }

    public MigrationSystemNotHealthyException(String msg, Throwable th) {
        super(msg, th);
    }
}
