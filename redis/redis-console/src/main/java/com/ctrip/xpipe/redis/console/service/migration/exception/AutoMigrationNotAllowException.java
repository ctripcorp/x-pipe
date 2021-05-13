package com.ctrip.xpipe.redis.console.service.migration.exception;

import com.ctrip.xpipe.redis.console.exception.RedisConsoleException;

/**
 * @author lishanglin
 * date 2021/5/13
 */
public class AutoMigrationNotAllowException extends RedisConsoleException {

    public AutoMigrationNotAllowException() {
        super("auto migration not allow");
    }

}
