package com.ctrip.xpipe.redis.console.healthcheck.action;


/**
 * @author chen.zhu
 * <p>
 * Sep 03, 2018
 */
public interface HealthEventProcessor {

    void onEvent(AbstractInstanceEvent event) throws HealthEventProcessorException;
}
