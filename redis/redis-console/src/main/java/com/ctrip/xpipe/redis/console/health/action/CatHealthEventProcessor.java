package com.ctrip.xpipe.redis.console.health.action;

import org.springframework.stereotype.Component;

import com.ctrip.xpipe.api.monitor.EventMonitor;

/**
 * @author wenchao.meng
 *         <p>
 *         May 05, 2017
 */
@Component
public class CatHealthEventProcessor implements HealthEventProcessor{

    private static final String TYPE = "HealthEvent";

    @Override
    public void onEvent(AbstractInstanceEvent instanceEvent) throws HealthEventProcessorException {
        EventMonitor.DEFAULT.logEvent(TYPE, String.format("%s-%s", instanceEvent.getClass().getSimpleName(), instanceEvent.getHostPort()));
    }
}
