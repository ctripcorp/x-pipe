package com.ctrip.xpipe.redis.console.healthcheck.nonredis.cluster;

import com.ctrip.xpipe.redis.checker.ClusterHealthManager;

import java.util.Map;
import java.util.Set;

public interface ClusterHealthMonitorManager extends ClusterHealthManager {

    void updateHealthCheckWarningShards(Map<String, Set<String>> warningClusterShards);

    void outerClientMasterDown(String clusterId, String shardId);

    void outerClientMasterUp(String clusterId, String shardId);

    Set<String> getWarningClusters(ClusterHealthState state);

}
