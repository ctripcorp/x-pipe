package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction;

import com.ctrip.xpipe.redis.checker.healthcheck.ActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.delay.AbstractDelayActionListener;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.delay.DelayActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.delay.DelayActionListener;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.ping.PingActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.ping.PingActionListener;
import com.google.common.collect.Maps;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public abstract class AbstractDelayPingActionCollector implements DelayPingActionCollector {

    protected Map<RedisHealthCheckInstance, HealthStatus> allHealthStatus = Maps.newConcurrentMap();

    protected PingActionListener pingActionListener = new AbstractDelayPingActionCollector.CollectorPingActionListener();

    protected DelayActionListener delayActionListener = new AbstractDelayPingActionCollector.CollectorDelayActionListener();

    protected abstract HealthStatus createOrGetHealthStatus(RedisHealthCheckInstance instance);

    protected void removeHealthStatus(HealthCheckAction action) {
        HealthStatus healthStatus = allHealthStatus.remove(action.getActionInstance());
        if(healthStatus != null) {
            healthStatus.stop();
        }
    }

    @Override
    public boolean supportInstance(RedisHealthCheckInstance instance) {
        return true;
    }

    @Override
    public PingActionListener createPingActionListener() {
        return pingActionListener;
    }

    @Override
    public DelayActionListener createDelayActionListener() {
        return delayActionListener;
    }

    protected class CollectorPingActionListener implements PingActionListener {

        @Override
        public void onAction(PingActionContext pingActionContext) {
            HealthStatus healthStatus = createOrGetHealthStatus(pingActionContext.instance());
            if (!pingActionContext.isSuccess()) {
                if (pingActionContext.getCause().getMessage().contains("LOADING")) {
                    healthStatus.loading();
                }
                return;
            }

            if (pingActionContext.getResult()) {
                healthStatus.pong();
            } else {
                if(healthStatus.getState() == HEALTH_STATE.UNKNOWN) {
                    healthStatus.pongInit();
                }
            }
        }

        @Override
        public boolean worksfor(ActionContext t) {
            return t instanceof PingActionContext;
        }

        @Override
        public void stopWatch(HealthCheckAction action) {
            removeHealthStatus(action);
        }
    }


    protected class CollectorDelayActionListener extends AbstractDelayActionListener implements DelayActionListener {

        @Override
        public void onAction(DelayActionContext context) {
            long delayNano = context.getResult();
            createOrGetHealthStatus(context.instance()).delay(TimeUnit.NANOSECONDS.toMillis(delayNano));
        }

        @Override
        public boolean worksfor(ActionContext t) {
            return t instanceof DelayActionContext;
        }

        @Override
        public void stopWatch(HealthCheckAction action) {
            removeHealthStatus(action);
        }
    }

}
