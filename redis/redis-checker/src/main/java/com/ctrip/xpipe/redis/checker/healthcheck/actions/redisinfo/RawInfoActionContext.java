package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisinfo;

import com.ctrip.xpipe.redis.checker.healthcheck.AbstractActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;

/**
 * @author Slight
 * <p>
 * Jun 01, 2021 3:22 PM
 */
public class RawInfoActionContext extends AbstractActionContext<String, RedisHealthCheckInstance> {

    public RawInfoActionContext(RedisHealthCheckInstance instance, String info) {
        super(instance, info);
    }

    public RawInfoActionContext(RedisHealthCheckInstance instance, Throwable t) {
        super(instance, t);
    }
}
