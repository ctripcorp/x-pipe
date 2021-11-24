package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.processor;


import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.AbstractInstanceEvent;

/**
 * @author chen.zhu
 * <p>
 * Sep 03, 2018
 */
public interface HealthEventProcessor {

    void onEvent(AbstractInstanceEvent event) throws HealthEventProcessorException;
}
