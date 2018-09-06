package com.ctrip.xpipe.redis.console.healthcheck.delay;

import com.ctrip.xpipe.redis.console.healthcheck.AbstractActionContext;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;

/**
 * @author chen.zhu
 * <p>
 * Sep 06, 2018
 */
public class DelayActionContext extends AbstractActionContext<Long> {

    public DelayActionContext(RedisHealthCheckInstance instance, Long delayNano) {
        super(instance, delayNano);
    }
}
