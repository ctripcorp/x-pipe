package com.ctrip.xpipe.redis.console.service.migration.exception;

import com.ctrip.xpipe.redis.console.exception.RedisConsoleException;

public class MigrationNotSupportException extends RedisConsoleException {

    public MigrationNotSupportException(String clusterName) {
        super(String.format("cluster %s not support migration", clusterName));
    }

    public MigrationNotSupportException(long clusterId) {
        super(String.format("cluster %s not support migration", clusterId));
    }

}
