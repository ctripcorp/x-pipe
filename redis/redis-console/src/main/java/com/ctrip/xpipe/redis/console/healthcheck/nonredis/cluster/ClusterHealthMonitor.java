package com.ctrip.xpipe.redis.console.healthcheck.nonredis.cluster;


public interface ClusterHealthMonitor {

    String getClusterId();

    void healthCheckMasterDown(String shardId);

    void healthCheckMasterUp(String shardId);

    void outerClientMasterDown(String shardId);

    void outerClientMasterUp(String shardId);

    void addListener(Listener listener);

    void removeListener(Listener listener);

    ClusterHealthState getState();

    interface Listener {

        void stateChange(String clusterId, ClusterHealthState pre, ClusterHealthState current);
    }
}
