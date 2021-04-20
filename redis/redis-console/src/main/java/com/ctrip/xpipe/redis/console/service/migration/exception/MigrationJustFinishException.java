package com.ctrip.xpipe.redis.console.service.migration.exception;

import com.ctrip.xpipe.redis.console.exception.RedisConsoleException;

/**
 * @author lishanglin
 * date 2021/4/17
 */
public class MigrationJustFinishException extends RedisConsoleException {

    public MigrationJustFinishException(String clusterName, String activeDcName) {
        super(String.format("migration just finish, %s now active dc is %s", clusterName, activeDcName));
    }

}