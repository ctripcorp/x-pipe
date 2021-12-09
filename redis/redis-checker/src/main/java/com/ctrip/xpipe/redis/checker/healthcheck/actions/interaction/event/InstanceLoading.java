package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event;

import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;

/**
 * @author Slight
 * <p>
 * Dec 07, 2021 3:52 PM
 */
public class InstanceLoading extends InstanceDown {

    public InstanceLoading(RedisHealthCheckInstance instance) {
        super(instance);
    }
}
