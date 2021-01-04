package com.ctrip.xpipe.redis.console.service.migration.exception;

import com.ctrip.xpipe.redis.console.exception.RedisConsoleException;

/**
 * @author lishanglin
 * date 2021/1/4
 */
public class MigrationCrossZoneException extends RedisConsoleException {

    public MigrationCrossZoneException(String clusterName, String currentDc, String targetDc) {
        super(String.format("%s migration %s->%s cross zone", clusterName, currentDc, targetDc));
    }

}
