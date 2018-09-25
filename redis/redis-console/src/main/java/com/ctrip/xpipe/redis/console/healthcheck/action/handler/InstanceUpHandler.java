package com.ctrip.xpipe.redis.console.healthcheck.action.handler;

import com.ctrip.xpipe.redis.console.healthcheck.action.event.AbstractInstanceEvent;
import com.ctrip.xpipe.redis.console.healthcheck.action.event.InstanceUp;

/**
 * @author chen.zhu
 * <p>
 * Sep 18, 2018
 */
public interface InstanceUpHandler extends HealthEventHandler {

    @Override
    default boolean supports(AbstractInstanceEvent event) {
        return event instanceof InstanceUp;
    }
}
