package com.ctrip.xpipe.redis.console.healthcheck.actions.delay;

import com.ctrip.xpipe.redis.console.healthcheck.AbstractActionContext;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;

/**
 * @author chen.zhu
 * <p>
 * Sep 06, 2018
 */
public class DelayActionContext extends AbstractActionContext<Long> {

    public DelayActionContext(RedisHealthCheckInstance instance, Long delay) {
        super(instance, delay);
    }
}
