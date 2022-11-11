package com.ctrip.xpipe.redis.checker.healthcheck.actions.delay;

import com.ctrip.xpipe.redis.checker.healthcheck.AbstractActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;

/**
 * @author chen.zhu
 * <p>
 * Sep 06, 2018
 */
public class DelayActionContext extends AbstractActionContext<Long, RedisHealthCheckInstance> {

    public DelayActionContext(RedisHealthCheckInstance instance, Long delay) {
        super(instance, delay);
    }

    public String getDelayType() {
        return "default";
    }

}
