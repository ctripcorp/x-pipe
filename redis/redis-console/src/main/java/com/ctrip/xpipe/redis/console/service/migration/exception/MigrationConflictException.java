package com.ctrip.xpipe.redis.console.service.migration.exception;

import com.ctrip.xpipe.redis.console.exception.RedisConsoleException;

/**
 * @author lishanglin
 * date 2020/12/28
 */
public class MigrationConflictException extends RedisConsoleException {

    public MigrationConflictException(String clusterName, String myTargetIdc, String currentTargetIdc) {
        super(String.format("%s migration conflict, my target dc %s, but current migration target dc %s", clusterName, myTargetIdc, currentTargetIdc));
    }

}
