package com.ctrip.xpipe.redis.console.healthcheck.actions.ping;

import com.ctrip.xpipe.redis.console.healthcheck.AbstractActionContext;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;

/**
 * @author chen.zhu
 * <p>
 * Sep 06, 2018
 */
public class PingActionContext extends AbstractActionContext<Boolean> {

    public PingActionContext(RedisHealthCheckInstance instance, Boolean pong) {
        super(instance, pong);
    }
}
