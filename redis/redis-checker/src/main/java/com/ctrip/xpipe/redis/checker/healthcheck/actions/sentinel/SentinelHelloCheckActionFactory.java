package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.Persistence;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.config.CheckerDbConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.leader.AbstractRedisLeaderAwareHealthCheckActionFactory;
import com.ctrip.xpipe.redis.checker.healthcheck.leader.SiteLeaderAwareHealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.util.ClusterTypeSupporterSeparator;
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
public class SentinelHelloCheckActionFactory extends AbstractRedisLeaderAwareHealthCheckActionFactory implements OneWaySupport, BiDirectionSupport {

    private Map<ClusterType, List<SentinelHelloCollector>> collectorsByClusterType;

    private Map<ClusterType, List<SentinelActionController>> controllersByClusterType;

    private CheckerDbConfig checkerDbConfig;

    private Persistence persistence;

    @Autowired
    public SentinelHelloCheckActionFactory(List<SentinelHelloCollector> collectors, List<SentinelActionController> controllers,
                                           CheckerConfig checkerConfig, CheckerDbConfig checkerDbConfig, Persistence persistence) {
        this.checkerDbConfig = checkerDbConfig;
        this.persistence = persistence;
        this.collectorsByClusterType = ClusterTypeSupporterSeparator.divideByClusterType(collectors);
        this.controllersByClusterType = ClusterTypeSupporterSeparator.divideByClusterType(controllers);
    }

    @Override
    public SiteLeaderAwareHealthCheckAction create(RedisHealthCheckInstance instance) {
        SentinelHelloCheckAction action = new SentinelHelloCheckAction(scheduled, instance, executors, checkerDbConfig, persistence);
        ClusterType clusterType = instance.getCheckInfo().getClusterType();
        action.addListeners(collectorsByClusterType.get(clusterType));
        action.addControllers(controllersByClusterType.get(clusterType));
        return action;
    }

    @Override
    public Class<? extends SiteLeaderAwareHealthCheckAction> support() {
        return SentinelHelloCheckAction.class;
    }

    @VisibleForTesting
    protected SentinelHelloCheckActionFactory setCheckerDbConfig(CheckerDbConfig checkerDbConfig) {
        this.checkerDbConfig = checkerDbConfig;
        return this;
    }

    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return Lists.newArrayList(ALERT_TYPE.SENTINEL_MONITOR_INCONSIS, ALERT_TYPE.SENTINEL_MONITOR_REDUNDANT_REDIS);
    }
}
