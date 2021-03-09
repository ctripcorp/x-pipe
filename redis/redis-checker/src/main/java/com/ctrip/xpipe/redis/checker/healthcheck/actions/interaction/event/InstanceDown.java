package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event;

import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;

/**
 * @author chen.zhu
 * <p>
 * Sep 06, 2018
 */
public class InstanceDown extends AbstractInstanceEvent {

    public InstanceDown(RedisHealthCheckInstance instance) {
        super(instance);
    }
}
