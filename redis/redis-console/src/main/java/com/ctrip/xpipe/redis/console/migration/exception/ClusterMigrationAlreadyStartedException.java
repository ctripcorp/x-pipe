package com.ctrip.xpipe.redis.console.migration.exception;

import com.ctrip.xpipe.exception.XpipeRuntimeException;

/**
 * @author lishanglin
 * date 2020/12/20
 */
public class ClusterMigrationAlreadyStartedException extends XpipeRuntimeException {

    private static final long serialVersionUID = 1L;

    public ClusterMigrationAlreadyStartedException(long eventId, String clusterName) {
        super(String.format("%d-%s already start, skip", eventId, clusterName));
    }

}
