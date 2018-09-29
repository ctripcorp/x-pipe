package com.ctrip.xpipe.redis.console.healthcheck.action;


import com.ctrip.xpipe.redis.console.healthcheck.action.event.AbstractInstanceEvent;

/**
 * @author chen.zhu
 * <p>
 * Sep 03, 2018
 */
public interface HealthEventProcessor {

    void onEvent(AbstractInstanceEvent event) throws HealthEventProcessorException;
}
