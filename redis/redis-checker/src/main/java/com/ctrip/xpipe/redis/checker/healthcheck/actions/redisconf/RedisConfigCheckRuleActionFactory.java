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
public class RedisConfigCheckRuleActionFactory extends AbstractRedisConfigCheckRuleActionFactory implements OneWaySupport {

    @Override
    public SiteLeaderAwareHealthCheckAction create(RedisHealthCheckInstance instance) {
//        List<RedisConfigCheckRule> configCheckRules = new LinkedList<>();
//        instance.getCheckInfo().getRedisConfigCheckRules().stream().filter(redisConfigCheckRule -> CONFIG_CHECKER_TYPE.equals(redisConfigCheckRule.getCheckType())).forEach(configCheckRules::add);
        return new RedisConfigCheckRuleAction(scheduled, instance, executors, alertManager, filterNonConifgRule(instance.getCheckInfo().getRedisConfigCheckRules()));
    }

    @Override
    public Class<? extends SiteLeaderAwareHealthCheckAction> support() {
        return RedisConfigCheckRuleAction.class;
    }
}
