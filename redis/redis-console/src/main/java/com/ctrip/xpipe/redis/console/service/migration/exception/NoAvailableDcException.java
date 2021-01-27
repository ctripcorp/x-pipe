package com.ctrip.xpipe.redis.console.service.migration.exception;

import com.ctrip.xpipe.redis.console.exception.RedisConsoleException;

/**
 * @author lishanglin
 * date 2020/12/28
 */
public class NoAvailableDcException extends RedisConsoleException {

    public NoAvailableDcException(String clusterName) {
        super("no available dc for migration for " + clusterName);
    }

}
