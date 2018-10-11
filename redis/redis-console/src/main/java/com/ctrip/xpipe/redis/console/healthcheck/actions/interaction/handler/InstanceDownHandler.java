package com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.handler;

import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.event.AbstractInstanceEvent;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.event.InstanceDown;

/**
 * @author chen.zhu
 * <p>
 * Sep 18, 2018
 */
public interface InstanceDownHandler extends HealthEventHandler {

    @Override
    default boolean supports(AbstractInstanceEvent event) {
        return event instanceof InstanceDown;
    }
}
