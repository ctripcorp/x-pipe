package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.handler;

import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.AbstractInstanceEvent;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.InstanceDown;
import org.springframework.stereotype.Component;

/**
 * @author chen.zhu
 * <p>
 * Sep 18, 2018
 */
@Component
public class DefaultInstanceDownHandler extends AbstractHealthEventHandler<InstanceDown> implements InstanceDownHandler {

    @Override
    protected void doHandle(InstanceDown instanceDown) {
        tryMarkDown(instanceDown);
    }

    @Override
    protected void doMarkDown(final AbstractInstanceEvent event) {
        doRealMarkDown(event);
    }
}
