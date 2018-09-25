package com.ctrip.xpipe.redis.console.healthcheck.action.handler;

import com.ctrip.xpipe.redis.console.healthcheck.action.event.InstanceUp;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * @author chen.zhu
 * <p>
 * Sep 18, 2018
 */
@Component
public class DefaultInstanceUpHandler extends AbstractHealthEventHandler<InstanceUp> implements InstanceUpHandler {

    @PostConstruct
    public void postConstruct() {
        setUpFinalStateSetterManager();
    }

    @Override
    protected void doHandle(InstanceUp instanceUp) {
        finalStateSetterManager.set(instanceUp.getInstance().getRedisInstanceInfo().getClusterShardHostport(), true);
    }

}
