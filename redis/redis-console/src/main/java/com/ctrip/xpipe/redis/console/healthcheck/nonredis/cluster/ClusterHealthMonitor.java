package com.ctrip.xpipe.redis.console.healthcheck.nonredis.cluster;


public interface ClusterHealthMonitor {

    String getClusterId();

    void becomeBetter(String shardId);

    void becomeWorse(String shardId);

    void addListener(Listener listener);

    void removeListener(Listener listener);

    ClusterHealthState getState();

    interface Listener {

        void stateChange(String clusterId, ClusterHealthState pre, ClusterHealthState current);
    }
}
