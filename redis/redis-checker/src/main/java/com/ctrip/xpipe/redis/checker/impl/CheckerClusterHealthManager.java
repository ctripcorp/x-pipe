package com.ctrip.xpipe.redis.checker.impl;

import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.checker.ClusterHealthManager;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.AbstractInstanceEvent;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.InstanceDown;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.InstanceSick;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.InstanceUp;
import com.ctrip.xpipe.utils.MapUtils;
import com.google.common.collect.Sets;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

/**
 * @author lishanglin
 * date 2021/3/12
 */
public class CheckerClusterHealthManager implements ClusterHealthManager {

    private Map<String, Set<String>> clusterWarningShards;

    private ExecutorService executors;

    public CheckerClusterHealthManager(ExecutorService executorService) {
        this.clusterWarningShards = new ConcurrentHashMap<>();
        this.executors = executorService;
    }

    @Override
    public void healthCheckMasterDown(RedisHealthCheckInstance instance) {
        RedisInstanceInfo info = instance.getCheckInfo();
        Set<String> warningShards = MapUtils.getOrCreate(clusterWarningShards, info.getClusterId(), Sets::newConcurrentHashSet);
        warningShards.add(info.getShardId());
    }

    @Override
    public void healthCheckMasterUp(RedisHealthCheckInstance instance) {
        RedisInstanceInfo info = instance.getCheckInfo();
        Set<String> warningShards = MapUtils.getOrCreate(clusterWarningShards, info.getClusterId(), Sets::newConcurrentHashSet);
        warningShards.remove(info.getShardId());
    }

    @Override
    public Map<String, Set<String>> getAllClusterWarningShards() {
        return clusterWarningShards;
    }

    @Override
    public Observer createHealthStatusObserver() {

        return new Observer() {
            @Override
            public void update(Object args, Observable observable) {
                onInstanceStateChange((AbstractInstanceEvent) args);
            }
        };
    }

    protected void onInstanceStateChange(Object args) {

        executors.execute(new AbstractExceptionLogTask() {

            @Override
            protected void doRun() {
                AbstractInstanceEvent event = (AbstractInstanceEvent) args;
                if (event.getInstance().getCheckInfo().getClusterType().supportMultiActiveDC()) {
                    // only care about the master status for single active dc cluster
                    return;
                }
                if(!event.getInstance().getCheckInfo().isMaster()) {
                    return;
                }
                if(event instanceof InstanceSick || event instanceof InstanceDown) {
                    healthCheckMasterDown(event.getInstance());
                } else if(event instanceof InstanceUp) {
                    healthCheckMasterUp(event.getInstance());
                }
            }
        });

    }

}
