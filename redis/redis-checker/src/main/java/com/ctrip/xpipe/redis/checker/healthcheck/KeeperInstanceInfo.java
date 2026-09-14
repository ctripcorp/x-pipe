package com.ctrip.xpipe.redis.checker.healthcheck;

import com.ctrip.xpipe.endpoint.HostPort;

/**
 * Metadata needed by Keeper-specific health checks.
 */
public interface KeeperInstanceInfo extends CheckInfo {

    String getShardId();

    Long getShardDbId();

    String getDcId();

    HostPort getHostPort();
}
