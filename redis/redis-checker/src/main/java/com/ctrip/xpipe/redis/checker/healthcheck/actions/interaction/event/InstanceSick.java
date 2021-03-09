package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event;

import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;

/**
 * @author chen.zhu
 * <p>
 * Sep 18, 2018
 */
public class InstanceSick extends AbstractInstanceEvent {
    public InstanceSick(RedisHealthCheckInstance instance) {
        super(instance);
    }
}
