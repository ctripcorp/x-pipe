package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event;

import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HealthStatus;

/**
 * @author chen.zhu
 * <p>
 * Sep 06, 2018
 */
public class AbstractInstanceEvent {

    protected RedisHealthCheckInstance instance;

    protected HealthStatus currentStatus;

    public AbstractInstanceEvent(RedisHealthCheckInstance instance) {
        this.instance = instance;
    }

    public RedisHealthCheckInstance getInstance() {
        return instance;
    }

    public HealthStatus getCurrentStatus() {
        return currentStatus;
    }

    @Override
    public String toString() {
        return String.format("%s:%s", getInstance(), getClass().getSimpleName());
    }
}
