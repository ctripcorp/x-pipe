package com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.event;

import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;

public class InstanceHalfSick extends AbstractInstanceEvent {

    public InstanceHalfSick(RedisHealthCheckInstance instance) {
        super(instance);
    }
}
