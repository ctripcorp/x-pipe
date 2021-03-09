package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event;

import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;

/**
 * @author chen.zhu
 * <p>
 * Sep 06, 2018
 */
public class InstanceUp extends AbstractInstanceEvent {
    public InstanceUp(RedisHealthCheckInstance instance) {
        super(instance);
    }
}
