package com.ctrip.xpipe.redis.console.healthcheck.actions.sentinel;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.config.ConsoleDbConfig;
import com.ctrip.xpipe.redis.console.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.console.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.leader.AbstractLeaderAwareHealthCheckActionFactory;
import com.ctrip.xpipe.redis.console.healthcheck.leader.SiteLeaderAwareHealthCheckAction;
import com.ctrip.xpipe.redis.console.healthcheck.util.ClusterTypeSupporterSeparator;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * @author chen.zhu
 * <p>
 * Oct 09, 2018
 */
@Component
public class SentinelHelloCheckActionFactory extends AbstractLeaderAwareHealthCheckActionFactory implements OneWaySupport, BiDirectionSupport {

    private Map<ClusterType, List<SentinelHelloCollector>> collectorsByClusterType;

    private Map<ClusterType, List<SentinelActionController>> controllersByClusterType;

    private ConsoleDbConfig consoleDbConfig;

    private ClusterService clusterService;

    @Autowired
    public SentinelHelloCheckActionFactory(List<SentinelHelloCollector> collectors, List<SentinelActionController> controllers,
                                           ConsoleDbConfig consoleDbConfig, ClusterService clusterService) {
        this.consoleDbConfig = consoleDbConfig;
        this.clusterService = clusterService;
        this.collectorsByClusterType = ClusterTypeSupporterSeparator.divideByClusterType(collectors);
        this.controllersByClusterType = ClusterTypeSupporterSeparator.divideByClusterType(controllers);
    }

    @Override
    public SiteLeaderAwareHealthCheckAction create(RedisHealthCheckInstance instance) {
        SentinelHelloCheckAction action = new SentinelHelloCheckAction(scheduled, instance, executors, consoleDbConfig,
                clusterService);
        ClusterType clusterType = instance.getRedisInstanceInfo().getClusterType();
        action.addListeners(collectorsByClusterType.get(clusterType));
        action.addControllers(controllersByClusterType.get(clusterType));
        return action;
    }

    @Override
    public Class<? extends SiteLeaderAwareHealthCheckAction> support() {
        return SentinelHelloCheckAction.class;
    }

    @VisibleForTesting
    protected SentinelHelloCheckActionFactory setConsoleDbConfig(ConsoleDbConfig consoleDbConfig) {
        this.consoleDbConfig = consoleDbConfig;
        return this;
    }

    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return Lists.newArrayList(ALERT_TYPE.SENTINEL_MONITOR_INCONSIS, ALERT_TYPE.SENTINEL_MONITOR_REDUNDANT_REDIS);
    }
}
