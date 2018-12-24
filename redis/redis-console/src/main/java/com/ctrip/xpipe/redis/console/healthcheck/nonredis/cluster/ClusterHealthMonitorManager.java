package com.ctrip.xpipe.redis.console.healthcheck.nonredis.cluster;

import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;

import java.util.Set;

public interface ClusterHealthMonitorManager {

    void healthCheckMasterDown(RedisHealthCheckInstance instance);

    void healthCheckMasterUp(RedisHealthCheckInstance instance);

    void outterClientMasterDown(String clusterId, String shardId);

    void outterClientMasterUp(String clusterId, String shardId);

    Set<String> getWarningClusters(ClusterHealthState state);

    Observer createHealthStatusObserver();
}
