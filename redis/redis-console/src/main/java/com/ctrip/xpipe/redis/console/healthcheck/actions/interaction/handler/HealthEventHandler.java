package com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.handler;

import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.event.AbstractInstanceEvent;

/**
 * @author chen.zhu
 * <p>
 * Sep 18, 2018
 */
public interface HealthEventHandler {

    void handle(AbstractInstanceEvent event);

    boolean supports(AbstractInstanceEvent event);
}
