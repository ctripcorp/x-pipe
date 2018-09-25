package com.ctrip.xpipe.redis.console.healthcheck.action.handler;

import com.ctrip.xpipe.redis.console.healthcheck.action.HEALTH_STATE;
import com.ctrip.xpipe.redis.console.healthcheck.action.event.AbstractInstanceEvent;
import com.ctrip.xpipe.redis.console.healthcheck.action.event.InstanceDown;
import com.google.common.collect.Lists;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Sep 18, 2018
 */
@Component
public class DefaultInstanceDownHandler extends AbstractHealthEventHandler<InstanceDown> implements InstanceDownHandler {

    private static final List<HEALTH_STATE> satisfiedStates = Lists.newArrayList(HEALTH_STATE.DOWN, HEALTH_STATE.UNHEALTHY);

    @Override
    protected void doHandle(InstanceDown instanceDown) {
        tryMarkDown(instanceDown);
    }

    @Override
    protected List<HEALTH_STATE> getSatisfiedStates() {
        return satisfiedStates;
    }

    @Override
    protected void doMarkDown(final AbstractInstanceEvent event) {
        doRealMarkDown(event);
    }
}
