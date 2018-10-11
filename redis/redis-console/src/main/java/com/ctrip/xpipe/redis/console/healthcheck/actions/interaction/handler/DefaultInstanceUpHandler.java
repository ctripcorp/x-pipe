package com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.handler;

import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.event.InstanceUp;
import org.springframework.stereotype.Component;

/**
 * @author chen.zhu
 * <p>
 * Sep 18, 2018
 */
@Component
public class DefaultInstanceUpHandler extends AbstractHealthEventHandler<InstanceUp> implements InstanceUpHandler {

    @Override
    protected void doHandle(InstanceUp instanceUp) {
        doRealMarkUp(instanceUp);
    }

}
