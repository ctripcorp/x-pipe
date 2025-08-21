package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf;

import com.ctrip.xpipe.redis.checker.healthcheck.AbstractActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;

public class RedisConfigCheckRuleActionContext extends AbstractActionContext<String, RedisHealthCheckInstance> {

    private RedisCheckRule redisCheckRule;

    public RedisConfigCheckRuleActionContext(RedisHealthCheckInstance instance,  String resultMessage, RedisCheckRule redisCheckRule) {
        super(instance, resultMessage);
        this.redisCheckRule = redisCheckRule;
    }

    public RedisConfigCheckRuleActionContext(RedisHealthCheckInstance instance, Throwable t) {
        super(instance, t);
    }

    public RedisCheckRule getRedisCheckRule() {
        return redisCheckRule;
    }
}
