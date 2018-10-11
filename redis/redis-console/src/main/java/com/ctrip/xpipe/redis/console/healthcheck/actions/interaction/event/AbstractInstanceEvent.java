package com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.event;

import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;

/**
 * @author chen.zhu
 * <p>
 * Sep 06, 2018
 */
public class AbstractInstanceEvent {

    protected RedisHealthCheckInstance instance;

    public AbstractInstanceEvent(RedisHealthCheckInstance instance) {
        this.instance = instance;
    }

    public RedisHealthCheckInstance getInstance() {
        return instance;
    }

    @Override
    public String toString() {
        return String.format("%s:%s", getInstance(), getClass().getSimpleName());
    }
}
