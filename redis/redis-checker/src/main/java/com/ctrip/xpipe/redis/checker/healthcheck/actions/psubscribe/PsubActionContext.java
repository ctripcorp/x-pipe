package com.ctrip.xpipe.redis.checker.healthcheck.actions.psubscribe;

import com.ctrip.xpipe.redis.checker.healthcheck.AbstractActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;

public class PsubActionContext extends AbstractActionContext<String, RedisHealthCheckInstance> {
    public PsubActionContext(RedisHealthCheckInstance instance, String s) {
        super(instance, s);
    }
}
