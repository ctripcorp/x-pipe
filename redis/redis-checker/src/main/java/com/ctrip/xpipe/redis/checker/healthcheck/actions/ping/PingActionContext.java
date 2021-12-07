package com.ctrip.xpipe.redis.checker.healthcheck.actions.ping;

import com.ctrip.xpipe.redis.checker.healthcheck.AbstractActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;

/**
 * @author chen.zhu
 * <p>
 * Sep 06, 2018
 */
public class PingActionContext extends AbstractActionContext<Boolean, RedisHealthCheckInstance> {

    public PingActionContext(RedisHealthCheckInstance instance, Boolean pong) {
        super(instance, pong);
    }

    public PingActionContext(RedisHealthCheckInstance instance, Boolean pong, Throwable t) {
        super(instance, t);
        c = pong;
    }
}
