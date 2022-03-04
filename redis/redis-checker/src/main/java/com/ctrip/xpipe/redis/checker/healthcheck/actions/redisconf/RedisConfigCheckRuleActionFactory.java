package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf;

import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.leader.AbstractRedisLeaderAwareHealthCheckActionFactory;
import com.ctrip.xpipe.redis.checker.healthcheck.leader.SiteLeaderAwareHealthCheckAction;
import com.google.common.collect.Lists;
import org.springframework.stereotype.Component;

import java.util.LinkedList;
import java.util.List;

@Component
public class RedisConfigCheckRuleActionFactory extends AbstractRedisConfigCheckRuleActionFactory implements OneWaySupport, BiDirectionSupport {

    @Override
    public SiteLeaderAwareHealthCheckAction create(RedisHealthCheckInstance instance) {
        RedisConfigCheckRuleAction redisConfigCheckRuleAction
                = new RedisConfigCheckRuleAction(scheduled, instance, executors, filterNonConifgRule(instance.getCheckInfo().getRedisCheckRules(), CONFIG_CHECKER_TYPE));
        redisConfigCheckRuleAction.addListener(new RedisConfigCheckRuleActionListener(alertManager));
        return redisConfigCheckRuleAction;
    }

    @Override
    public Class<? extends SiteLeaderAwareHealthCheckAction> support() {
        return RedisConfigCheckRuleAction.class;
    }

    @Override
    public boolean supportInstnace(RedisHealthCheckInstance instance) {
        List<RedisCheckRule> redisCheckRules = filterNonConifgRule(instance.getCheckInfo().getRedisCheckRules(), CONFIG_CHECKER_TYPE);
        if(redisCheckRules == null || redisCheckRules.isEmpty())
            return false;
        return true;
    }
}
