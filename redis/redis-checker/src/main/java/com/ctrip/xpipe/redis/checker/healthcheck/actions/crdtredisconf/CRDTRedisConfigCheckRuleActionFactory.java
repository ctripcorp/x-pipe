package com.ctrip.xpipe.redis.checker.healthcheck.actions.crdtredisconf;

import com.ctrip.xpipe.redis.checker.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf.AbstractRedisConfigCheckRuleActionFactory;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf.RedisConfigCheckRuleActionListener;
import com.ctrip.xpipe.redis.checker.healthcheck.leader.SiteLeaderAwareHealthCheckAction;
import org.springframework.stereotype.Component;

@Component
public class CRDTRedisConfigCheckRuleActionFactory extends AbstractRedisConfigCheckRuleActionFactory implements BiDirectionSupport {
    @Override
    public SiteLeaderAwareHealthCheckAction create(RedisHealthCheckInstance instance) {
        CRDTRedisConfigCheckRuleAction crdtRedisConfigCheckRuleAction =
                new CRDTRedisConfigCheckRuleAction(scheduled, instance, executors, filterNonConifgRule(instance.getCheckInfo().getRedisCheckRules()));
        crdtRedisConfigCheckRuleAction.addListener(new RedisConfigCheckRuleActionListener(alertManager));
        return crdtRedisConfigCheckRuleAction;
    }

    @Override
    public Class<? extends SiteLeaderAwareHealthCheckAction> support() {
        return CRDTRedisConfigCheckRuleAction.class;
    }
}
