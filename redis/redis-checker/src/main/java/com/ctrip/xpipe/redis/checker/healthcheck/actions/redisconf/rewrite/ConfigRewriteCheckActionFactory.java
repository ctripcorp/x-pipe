package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf.rewrite;

import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.leader.AbstractRedisLeaderAwareHealthCheckActionFactory;
import com.ctrip.xpipe.redis.checker.healthcheck.leader.SiteLeaderAwareHealthCheckAction;
import com.google.common.collect.Lists;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Oct 06, 2018
 */

@Component
public class ConfigRewriteCheckActionFactory extends AbstractRedisLeaderAwareHealthCheckActionFactory implements OneWaySupport, BiDirectionSupport {

    @Override
    public SiteLeaderAwareHealthCheckAction create(RedisHealthCheckInstance instance) {
        return new ConfigRewriteCheckAction(scheduled, instance, executors, alertManager);
    }

    @Override
    public Class<? extends SiteLeaderAwareHealthCheckAction> support() {
        return ConfigRewriteCheckAction.class;
    }

    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return Lists.newArrayList(ALERT_TYPE.REDIS_CONF_REWRITE_FAILURE);
    }
}
