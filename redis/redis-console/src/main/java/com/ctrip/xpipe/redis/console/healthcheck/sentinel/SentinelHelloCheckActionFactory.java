package com.ctrip.xpipe.redis.console.healthcheck.sentinel;

import com.ctrip.xpipe.redis.console.config.ConsoleDbConfig;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.redisconf.AbstractCDLAHealthCheckActionFactory;
import com.ctrip.xpipe.redis.console.healthcheck.redisconf.CrossDcLeaderAwareHealthCheckAction;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Oct 09, 2018
 */
@Component
public class SentinelHelloCheckActionFactory extends AbstractCDLAHealthCheckActionFactory {

    @Autowired
    private List<SentinelHelloCollector> collectors;

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private ConsoleDbConfig consoleDbConfig;

    @Override
    public CrossDcLeaderAwareHealthCheckAction create(RedisHealthCheckInstance instance) {
        SentinelHelloCheckAction action = new SentinelHelloCheckAction(scheduled, instance, executors, metaCache, consoleDbConfig);
        for(SentinelHelloCollector collector : collectors) {
            action.addListener(collector.getSentinelHelloActionListener());
        }
        return action;
    }

    @Override
    public Class<? extends CrossDcLeaderAwareHealthCheckAction> support() {
        return SentinelHelloCheckAction.class;
    }

    @VisibleForTesting
    protected SentinelHelloCheckActionFactory setMetaCache(MetaCache metaCache) {
        this.metaCache = metaCache;
        return this;
    }

    @VisibleForTesting
    protected SentinelHelloCheckActionFactory setConsoleDbConfig(ConsoleDbConfig consoleDbConfig) {
        this.consoleDbConfig = consoleDbConfig;
        return this;
    }
}
