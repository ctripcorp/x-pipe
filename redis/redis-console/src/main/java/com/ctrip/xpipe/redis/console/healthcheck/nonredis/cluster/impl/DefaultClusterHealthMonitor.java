package com.ctrip.xpipe.redis.console.healthcheck.nonredis.cluster.impl;

import com.ctrip.xpipe.redis.console.healthcheck.crossdc.SafeLoop;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.cluster.ClusterHealthMonitor;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.cluster.ClusterHealthState;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class DefaultClusterHealthMonitor implements ClusterHealthMonitor {

    private String clusterId;

    private ShardService shardService;

    private AtomicReference<ClusterHealthState> state = new AtomicReference<>(ClusterHealthState.NORMAL);

    private Set<String> warningShards = Sets.newConcurrentHashSet();

    private List<Listener> listeners = Lists.newCopyOnWriteArrayList();

    public DefaultClusterHealthMonitor(String clusterId, ShardService shardService) {
        this.clusterId = clusterId;
        this.shardService = shardService;
    }

    @Override
    public String getClusterId() {
        return clusterId;
    }

    @Override
    public void becomeBetter(String shardId) {
        warningShards.remove(shardId);
        checkIfStateChange();
    }

    @Override
    public void becomeWorse(String shardId) {
        warningShards.add(shardId);
        checkIfStateChange();
    }

    @Override
    public void addListener(Listener listener) {
        listeners.add(listener);
    }

    @Override
    public void removeListener(Listener listener) {
        listeners.remove(listener);
    }

    @Override
    public ClusterHealthState getState() {
        return state.get();
    }

    private void checkIfStateChange() {
        int warningShardNums = warningShards.size();
        int totalShards = shardService.findAllShardNamesByClusterName(getClusterId()).size();
        ClusterHealthState current = ClusterHealthState.getState(totalShards, warningShardNums);
        ClusterHealthState prev = state.get();
        if(!prev.equals(current) && state.compareAndSet(prev, current)) {
            notifyListeners(prev, current);
        }
    }

    private void notifyListeners(ClusterHealthState pre, ClusterHealthState cur) {
        new SafeLoop<Listener>(listeners) {
            @Override
            protected void doRun0(Listener listener) {
                listener.stateChange(getClusterId(), pre, cur);
            }
        }.run();
    }

}
