package com.ctrip.xpipe.redis.console.service.migration.exception;

import com.ctrip.xpipe.redis.console.exception.RedisConsoleException;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;

/**
 * @author lishanglin
 * date 2021/4/17
 */
public class UnexpectMigrationDataException extends RedisConsoleException {

    public UnexpectMigrationDataException(ClusterTbl clusterTbl, String msg) {
        super(String.format("unexpect migration data for %s-%d: %s", clusterTbl.getClusterName(), clusterTbl.getMigrationEventId(), msg));
    }

}
