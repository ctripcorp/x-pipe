package com.ctrip.xpipe.redis.console.healthcheck.actions.sentinel;

import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.config.ConsoleDbConfig;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.leader.AbstractLeaderAwareHealthCheckActionFactory;
import com.ctrip.xpipe.redis.console.healthcheck.leader.SiteLeaderAwareHealthCheckAction;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Oct 09, 2018
 */
@Component
public class SentinelHelloCheckActionFactory extends AbstractLeaderAwareHealthCheckActionFactory {

    @Autowired
    private List<SentinelHelloCollector> collectors;

    @Autowired
    private ConsoleDbConfig consoleDbConfig;

    @Autowired
    private ClusterService clusterService;

    @Override
    public SiteLeaderAwareHealthCheckAction create(RedisHealthCheckInstance instance) {
        SentinelHelloCheckAction action = new SentinelHelloCheckAction(scheduled, instance, executors, consoleDbConfig,
                clusterService);
        for(SentinelHelloCollector collector : collectors) {
            action.addListener(collector);
        }
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
