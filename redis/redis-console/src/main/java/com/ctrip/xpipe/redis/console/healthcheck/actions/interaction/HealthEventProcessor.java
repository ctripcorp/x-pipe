package com.ctrip.xpipe.redis.console.healthcheck.actions.interaction;


import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.event.AbstractInstanceEvent;

/**
 * @author chen.zhu
 * <p>
 * Sep 03, 2018
 */
public interface HealthEventProcessor {

    void onEvent(AbstractInstanceEvent event) throws HealthEventProcessorException;
}
