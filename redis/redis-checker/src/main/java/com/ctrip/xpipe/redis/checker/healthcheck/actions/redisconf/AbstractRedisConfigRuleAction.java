package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf;

import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.leader.AbstractLeaderAwareHealthCheckAction;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public abstract class AbstractRedisConfigRuleAction extends AbstractLeaderAwareHealthCheckAction<RedisHealthCheckInstance> {

    protected List<RedisCheckRule> redisCheckRules;

    public static final String CONFIG_CHECK_NAME = "configCheckName";

    public static final String EXPECTED_VAULE = "expectedVaule";


    public AbstractRedisConfigRuleAction(ScheduledExecutorService scheduled, RedisHealthCheckInstance instance, ExecutorService executors, List<RedisCheckRule> redisCheckRules) {
        super(scheduled, instance, executors);
        this.redisCheckRules = redisCheckRules;
    }

    public List<RedisCheckRule> getRedisConfigCheckRules() {
        return redisCheckRules;
    }

    @Override
    protected int getBaseCheckInterval() {
        return getActionInstance().getHealthCheckConfig().getNonCoreCheckIntervalMilli();
    }
}
