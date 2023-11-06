package com.ctrip.xpipe.redis.checker.healthcheck;

import com.ctrip.xpipe.endpoint.ClusterShardHostPort;
import com.ctrip.xpipe.endpoint.HostPort;


public interface KeeperInstanceInfo extends CheckInfo {

    ClusterShardHostPort getClusterShardHostport();

    String getShardId();

    String getDcId();

    boolean isActive();

    HostPort getHostPort();

}
