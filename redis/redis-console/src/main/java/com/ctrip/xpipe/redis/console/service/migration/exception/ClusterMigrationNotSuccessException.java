package com.ctrip.xpipe.redis.console.service.migration.exception;

import com.ctrip.xpipe.redis.console.exception.RedisConsoleException;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;

/**
 * @author lishanglin
 * date 2020/12/31
 */
public class ClusterMigrationNotSuccessException extends RedisConsoleException {

    public ClusterMigrationNotSuccessException(String clusterName, MigrationStatus status) {
        super(String.format("%s migration stopped with %s", clusterName, status.name()));
    }

}
