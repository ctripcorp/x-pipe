package com.ctrip.xpipe.redis.console.healthcheck.nonredis.cluster.impl;

import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.event.AbstractInstanceEvent;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.event.InstanceDown;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.event.InstanceSick;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.event.InstanceUp;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.cluster.ClusterHealthMonitor;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.cluster.ClusterHealthMonitorManager;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.cluster.ClusterHealthState;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.cluster.LeveledEmbededSet;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Lazy
@Component
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
public class DefaultClusterHealthMonitorManager implements ClusterHealthMonitorManager {

    private static final Logger logger = LoggerFactory.getLogger(DefaultClusterHealthMonitorManager.class);

    @Autowired
    private ShardService shardService;

    private ConcurrentMap<String, DefaultClusterHealthMonitor> monitors = Maps.newConcurrentMap();

    private LeveledEmbededSet<String> warningClusters = new DefaultLeveledEmbededSet<>();

    private ExecutorService executors = Executors.newFixedThreadPool(Math.max(1, Math.min(2, OsUtils.getCpuCount()/2)),
            XpipeThreadFactory.create(DefaultClusterHealthMonitorManager.class.getSimpleName()));

    private ClusterHealthMonitorListener clusterHealthMonitorListener = new ClusterHealthMonitorListener();

    @Override
    public void healthCheckMasterDown(RedisHealthCheckInstance instance) {
        DefaultClusterHealthMonitor monitor = getOrCreate(instance.getRedisInstanceInfo().getClusterId());
        monitor.healthCheckMasterDown(instance.getRedisInstanceInfo().getShardId());
    }

    @Override
    public void healthCheckMasterUp(RedisHealthCheckInstance instance) {
        if(!monitors.containsKey(instance.getRedisInstanceInfo().getClusterId())) {
            logger.debug("[healthCheckMasterUp] Cluster is not warned before: {}", instance.getRedisInstanceInfo().getClusterId());
            return;
        }
        DefaultClusterHealthMonitor monitor = getOrCreate(instance.getRedisInstanceInfo().getClusterId());
        monitor.healthCheckMasterUp(instance.getRedisInstanceInfo().getShardId());
    }

    @Override
    public void outerClientMasterDown(String clusterId, String shardId) {
        DefaultClusterHealthMonitor monitor = getOrCreate(clusterId);
        monitor.outerClientMasterDown(shardId);
    }

    @Override
    public void outerClientMasterUp(String clusterId, String shardId) {
        if(!monitors.containsKey(clusterId)) {
            logger.debug("[outerClientMasterUp] Cluster is not warned before: {}", clusterId);
            return;
        }
        DefaultClusterHealthMonitor monitor = getOrCreate(clusterId);
        monitor.outerClientMasterUp(shardId);
    }

    @Override
    public Set<String> getWarningClusters(ClusterHealthState state) {
        if(state.equals(ClusterHealthState.NORMAL)) {
            return Sets.newHashSet();
        }
        return warningClusters.getThrough(state.getLevel()).getCurrentSet();
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

    private DefaultClusterHealthMonitor getOrCreate(String clusterId) {
        return MapUtils.getOrCreate(monitors, clusterId, new ObjectFactory<DefaultClusterHealthMonitor>() {
            @Override
            public DefaultClusterHealthMonitor create() {
                DefaultClusterHealthMonitor monitor = new DefaultClusterHealthMonitor(clusterId, shardService);
                monitor.addListener(clusterHealthMonitorListener);
                return monitor;
            }
        });
    }

    private void resumeCluster(String clusterId, int level) {
        warningClusters.resume(clusterId, level);
    }

    private class ClusterHealthMonitorListener implements ClusterHealthMonitor.Listener {
        @Override
        public void stateChange(String clusterId, ClusterHealthState pre, ClusterHealthState current) {
            if(pre.equals(current)) {
                return;
            }
            logger.info("[stateChange][{}] {} -> {}", clusterId, pre.name(), current.name());
            if(ClusterHealthState.NORMAL.equals(current)) {
                remove(clusterId);
                return;
            }
            resumeCluster(clusterId, current.getLevel());
        }
    }

    private void remove(String clusterId) {
        warningClusters.remove(clusterId);
        DefaultClusterHealthMonitor monitor = monitors.remove(clusterId);
        if(monitor != null) {
            monitor.removeListener(clusterHealthMonitorListener);
        }
    }

    private void onInstanceStateChange(Object args) {

        executors.execute(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() {
                AbstractInstanceEvent event = (AbstractInstanceEvent) args;
                if(!event.getInstance().getRedisInstanceInfo().isMaster()) {
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

    @VisibleForTesting
    protected DefaultClusterHealthMonitorManager setShardService(ShardService shardService) {
        this.shardService = shardService;
        return this;
    }
}
