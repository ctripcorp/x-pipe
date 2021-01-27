package com.ctrip.xpipe.redis.console.service.migration.exception;

import com.ctrip.xpipe.redis.console.exception.RedisConsoleException;

/**
 * @author lishanglin
 * date 2020/12/28
 */
public class MigrationNoNeedException extends RedisConsoleException {

    public MigrationNoNeedException(String clusterName) {
        super("migration no need for " + clusterName);
    }

}
