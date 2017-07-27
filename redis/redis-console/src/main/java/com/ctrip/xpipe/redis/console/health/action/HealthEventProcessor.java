package com.ctrip.xpipe.redis.console.health.action;

/**
 * @author wenchao.meng
 *         <p>
 *         May 04, 2017
 */
public interface HealthEventProcessor {

    void onEvent(AbstractInstanceEvent instanceEvent) throws HealthEventProcessorException;
}
