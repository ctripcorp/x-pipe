package com.ctrip.xpipe.redis.console.healthcheck.nonredis.cluster.impl;

import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.cluster.ClusterHealthMonitorManager;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.cluster.ClusterHealthState;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.google.common.collect.Sets;
import jakarta.annotation.PostConstruct;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;

@Component
@Profile(AbstractProfile.PROFILE_NAME_TEST)
public class UnitTestClusterHealthMonitorManager implements ClusterHealthMonitorManager {

    private Set<String> warningClusters = Sets.newConcurrentHashSet();

    @PostConstruct
    public void postConstruction() {
        warningClusters.add("cluster1");
    }

    @Override
    public void healthCheckMasterDown(RedisHealthCheckInstance instance) {

    }

    @Override
    public void healthCheckMasterUp(RedisHealthCheckInstance instance) {

    }

    @Override
    public void outerClientMasterDown(String clusterId, String shardId) {

    }

    @Override
    public void outerClientMasterUp(String clusterId, String shardId) {

    }

    @Override
    public Set<String> getWarningClusters(ClusterHealthState state) {
        if(state.equals(ClusterHealthState.HALF_DOWN)
                || state.equals(ClusterHealthState.QUARTER_DOWN)
                || state.equals(ClusterHealthState.LEAST_ONE_DOWN)) {
            return Sets.newHashSet(warningClusters);
        }
        return Sets.newHashSet();
    }

    @Override
    public Observer createHealthStatusObserver() {
        return null;
    }

    @Override
    public void updateHealthCheckWarningShards(Map<String, Set<String>> warningClusterShards) {

    }

    @Override
    public Map<String, Set<String>> getAllClusterWarningShards() {
        return null;
    }
}
