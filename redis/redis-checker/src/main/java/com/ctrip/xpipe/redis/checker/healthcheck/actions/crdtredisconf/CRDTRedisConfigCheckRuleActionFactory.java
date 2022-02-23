package com.ctrip.xpipe.redis.checker.healthcheck.actions.crdtredisconf;

import com.ctrip.xpipe.redis.checker.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf.AbstractRedisConfigCheckRuleActionFactory;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf.RedisConfigCheckRule;
import com.ctrip.xpipe.redis.checker.healthcheck.leader.SiteLeaderAwareHealthCheckAction;
import org.springframework.stereotype.Component;

import java.util.LinkedList;
import java.util.List;

@Component
public class CRDTRedisConfigCheckRuleActionFactory extends AbstractRedisConfigCheckRuleActionFactory implements BiDirectionSupport {
    @Override
    public SiteLeaderAwareHealthCheckAction create(RedisHealthCheckInstance instance) {
//        List<RedisConfigCheckRule> configCheckRules = new LinkedList<>();
//        instance.getCheckInfo().getRedisConfigCheckRules().stream().filter(redisConfigCheckRule -> CONFIG_CHECKER_TYPE.equals(redisConfigCheckRule.getCheckType())).forEach(configCheckRules::add);
        return new CRDTRedisConfigCheckRuleAction(scheduled, instance, executors, alertManager, filterNonConifgRule(instance.getCheckInfo().getRedisConfigCheckRules()));
    }

    @Override
    public Class<? extends SiteLeaderAwareHealthCheckAction> support() {
        return CRDTRedisConfigCheckRuleAction.class;
    }
}
