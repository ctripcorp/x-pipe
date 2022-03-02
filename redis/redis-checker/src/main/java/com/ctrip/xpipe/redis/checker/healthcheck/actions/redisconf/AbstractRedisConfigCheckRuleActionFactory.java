package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf;

import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.leader.AbstractRedisLeaderAwareHealthCheckActionFactory;
import com.google.common.collect.Lists;

import java.util.LinkedList;
import java.util.List;

public abstract class AbstractRedisConfigCheckRuleActionFactory extends AbstractRedisLeaderAwareHealthCheckActionFactory {
    protected static final String CONFIG_CHECKER_TYPE = "config";

    @Override
    public boolean supportInstnace(RedisHealthCheckInstance instance) {
        List<RedisCheckRule> redisCheckRules = filterNonConifgRule(instance.getCheckInfo().getRedisCheckRules());
        if(redisCheckRules == null || redisCheckRules.isEmpty())
            return false;
        return true;
    }

    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return Lists.newArrayList(ALERT_TYPE.REDIS_CONIFIG_CHECK_FAIL);
    }

    protected List<RedisCheckRule> filterNonConifgRule( List<RedisCheckRule> allConfigCheckRules) {
        List<RedisCheckRule> resultCheckRules = new LinkedList<>();
        allConfigCheckRules.stream().filter(redisCheckRule -> CONFIG_CHECKER_TYPE.equals(redisCheckRule.getCheckType())).forEach(resultCheckRules::add);
        return resultCheckRules;
    }

}
