package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.ActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.delay.AbstractDelayActionListener;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.delay.DelayActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.delay.DelayActionListener;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.delay.HeteroDelayActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.ping.PingActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.ping.PingActionListener;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Maps;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public abstract class AbstractDelayPingActionCollector implements DelayPingActionCollector {

    protected static final String currentDcId = FoundationService.DEFAULT.getDataCenter();

    protected Map<RedisHealthCheckInstance, HealthStatus> allHealthStatus = Maps.newConcurrentMap();

    protected PingActionListener pingActionListener = new AbstractDelayPingActionCollector.CollectorPingActionListener();

    protected DelayActionListener delayActionListener = new AbstractDelayPingActionCollector.CollectorDelayActionListener();

    protected abstract HealthStatus getHealthStatus(RedisHealthCheckInstance instance);

    protected void removeHealthStatus(HealthCheckAction action) {
        HealthStatus healthStatus = allHealthStatus.remove(action.getActionInstance());
        if(healthStatus != null) {
            healthStatus.stop();
        }
    }

    protected HealthStatus getHealthStatus(HostPort hostPort) {
        RedisHealthCheckInstance key = allHealthStatus.keySet().stream()
                .filter(instance -> instance.getCheckInfo().getHostPort().equals(hostPort))
                .findFirst().orElse(null);

        return null == key ? null : allHealthStatus.get(key);
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

    @Override
    @VisibleForTesting
    public Map<RedisHealthCheckInstance, HealthStatus> getAllInstancesHealthStatus() {
        return allHealthStatus;
    }

    protected class CollectorPingActionListener implements PingActionListener {

        @Override
        public void onAction(PingActionContext pingActionContext) {
            HealthStatus healthStatus = getHealthStatus(pingActionContext.instance());
            if (null == healthStatus) {
                return;
            }
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
            HealthStatus healthStatus = getHealthStatus(context.instance());
            if (null == healthStatus) {
                return;
            }
            if (context instanceof HeteroDelayActionContext)
                healthStatus.delay(TimeUnit.NANOSECONDS.toMillis(delayNano), ((HeteroDelayActionContext) context).getShardDbId());
            else
                healthStatus.delay(TimeUnit.NANOSECONDS.toMillis(delayNano));
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
