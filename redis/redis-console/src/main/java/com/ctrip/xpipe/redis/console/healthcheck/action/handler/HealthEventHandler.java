package com.ctrip.xpipe.redis.console.healthcheck.action.handler;

import com.ctrip.xpipe.redis.console.healthcheck.action.event.AbstractInstanceEvent;

/**
 * @author chen.zhu
 * <p>
 * Sep 18, 2018
 */
public interface HealthEventHandler {

    void handle(AbstractInstanceEvent event);

    boolean supports(AbstractInstanceEvent event);
}
