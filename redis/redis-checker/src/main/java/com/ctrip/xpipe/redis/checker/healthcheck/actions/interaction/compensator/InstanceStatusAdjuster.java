package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.compensator;

import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.concurrent.DefaultExecutorFactory;
import com.ctrip.xpipe.endpoint.ClusterShardHostPort;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.AggregatorPullService;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.ClusterActiveDcKey;
import com.ctrip.xpipe.redis.checker.healthcheck.stability.StabilityHolder;
import com.ctrip.xpipe.redis.checker.healthcheck.stability.StabilityInspector;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;


/**
 * @author lishanglin
 * date 2022/7/24
 */
@Component
public class InstanceStatusAdjuster {

    private InstanceHealthStatusCollector collector;

    private StabilityHolder siteStability;

    private CheckerConfig config;

    private AlertManager alertManager;

    private MetaCache metaCache;

    private ExecutorService executors;

    private AggregatorPullService aggregatorPullService;

    @Autowired
    public InstanceStatusAdjuster(InstanceHealthStatusCollector collector, AlertManager alertManager,
                                  MetaCache metaCache, StabilityHolder stabilityHolder, CheckerConfig checkerConfig, AggregatorPullService aggregatorPullService) {
        this.collector = collector;
        this.alertManager = alertManager;
        this.metaCache = metaCache;
        this.siteStability = stabilityHolder;
        this.config = checkerConfig;
        DefaultExecutorFactory executorFactory = new DefaultExecutorFactory("InstanceStatusAdjuster",
                config.getHealthMarkCompensateThreads(), config.getHealthMarkCompensateThreads(), new ThreadPoolExecutor.AbortPolicy());
        this.executors = executorFactory.createExecutorService();
        this.aggregatorPullService = aggregatorPullService;
    }

    public void adjustInstances(Set<HostPort> instances, long timeoutAtMilli) {
        Map<ClusterActiveDcKey, List<ClusterShardHostPort>> clusterInstances = new HashMap<>();
        for (HostPort instance : instances) {
            Pair<String, String> clusterShard = metaCache.findClusterShard(instance);
            String activeDc = metaCache.getActiveDc(clusterShard.getKey());
            List<ClusterShardHostPort> current = clusterInstances.getOrDefault(new ClusterActiveDcKey(clusterShard.getKey(), activeDc), new ArrayList<>());
            current.add(new ClusterShardHostPort(clusterShard.getKey(), clusterShard.getValue(), activeDc, instance));
        }
        clusterInstances.forEach((key, value) -> {
            new ClusterStatusAdjustCommand(key, value, timeoutAtMilli, siteStability, config, metaCache, aggregatorPullService).execute(executors);
        });
    }

}
