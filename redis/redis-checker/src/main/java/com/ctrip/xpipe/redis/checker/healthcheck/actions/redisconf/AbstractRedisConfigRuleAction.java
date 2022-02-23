package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf;

import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.leader.AbstractLeaderAwareHealthCheckAction;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public abstract class AbstractRedisConfigRuleAction extends AbstractLeaderAwareHealthCheckAction<RedisHealthCheckInstance> {

    protected List<RedisConfigCheckRule> redisConfigCheckRules;

    protected AlertManager alertManager;

    public AbstractRedisConfigRuleAction(ScheduledExecutorService scheduled, RedisHealthCheckInstance instance, ExecutorService executors, AlertManager alertManager, List<RedisConfigCheckRule> redisConfigCheckRules) {
        super(scheduled, instance, executors);
        this.redisConfigCheckRules = redisConfigCheckRules;
        this.alertManager = alertManager;
    }
}
