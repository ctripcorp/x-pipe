package com.ctrip.xpipe.redis.console.healthcheck.redisconf.rewrite;

import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.redisconf.AbstractCDLAHealthCheckActionFactory;
import com.ctrip.xpipe.redis.console.healthcheck.redisconf.CrossDcLeaderAwareHealthCheckAction;
import org.springframework.stereotype.Component;

/**
 * @author chen.zhu
 * <p>
 * Oct 06, 2018
 */

@Component
public class ConfigRewriteCheckActionFactory extends AbstractCDLAHealthCheckActionFactory {

    @Override
    public CrossDcLeaderAwareHealthCheckAction create(RedisHealthCheckInstance instance) {
        return new ConfigRewriteCheckAction(scheduled, instance, executors, alertManager);
    }

    @Override
    public Class<? extends CrossDcLeaderAwareHealthCheckAction> support() {
        return ConfigRewriteCheckAction.class;
    }
}
