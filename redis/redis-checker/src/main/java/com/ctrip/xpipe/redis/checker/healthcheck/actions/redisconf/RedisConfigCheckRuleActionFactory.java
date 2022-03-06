package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf;

import com.ctrip.xpipe.redis.checker.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.leader.SiteLeaderAwareHealthCheckAction;
import org.springframework.stereotype.Component;

@Component
public class RedisConfigCheckRuleActionFactory extends AbstractRedisConfigCheckRuleActionFactory implements OneWaySupport, BiDirectionSupport {

    private static final String CONFIG_CHECK_TYPE = "config";

    @Override
    public SiteLeaderAwareHealthCheckAction create(RedisHealthCheckInstance instance) {
        RedisConfigCheckRuleAction redisConfigCheckRuleAction
                = new RedisConfigCheckRuleAction(scheduled, instance, executors, filterNonConifgRule(instance.getCheckInfo().getRedisCheckRules(), CONFIG_CHECK_TYPE));
        redisConfigCheckRuleAction.addListener(new RedisConfigCheckRuleActionListener(alertManager));
        return redisConfigCheckRuleAction;
    }

    @Override
    public Class<? extends SiteLeaderAwareHealthCheckAction> support() {
        return RedisConfigCheckRuleAction.class;
    }


    @Override
    public String getCheckType() {
        return CONFIG_CHECK_TYPE;
    }
}
