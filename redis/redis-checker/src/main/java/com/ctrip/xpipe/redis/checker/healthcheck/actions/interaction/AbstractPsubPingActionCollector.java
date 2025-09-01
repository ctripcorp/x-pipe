package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.ActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.ping.PingActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.ping.PingActionListener;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.psubscribe.PsubActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.psubscribe.PsubActionListener;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.psubscribe.PsubPingActionCollector;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Map;
import java.util.Set;

public abstract class AbstractPsubPingActionCollector implements PsubPingActionCollector {

    protected Map<RedisHealthCheckInstance, HealthStatus> allHealthStatus = Maps.newConcurrentMap();

    protected Set<RedisHealthCheckInstance> instancePresentStatus = Sets.newConcurrentHashSet();

    protected PingActionListener pingActionListener = new AbstractPsubPingActionCollector.CollectorPingActionListener();

    protected PsubActionListener psubActionListener = new AbstractPsubPingActionCollector.CollectorPsubActionListener();

    protected abstract HealthStatus createOrGetHealthStatus(RedisHealthCheckInstance instance);

    protected synchronized void removeHealthStatus(HealthCheckAction<RedisHealthCheckInstance> action) {
        instancePresentStatus.remove(action.getActionInstance());
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
    public PingActionListener createPingActionListener(RedisHealthCheckInstance instance) {
        instancePresentStatus.add(instance);
        return pingActionListener;
    }

    @Override
    public PsubActionListener createPsubActionListener(RedisHealthCheckInstance instance) {
        instancePresentStatus.add(instance);
        return psubActionListener;
    }

    protected HealthStatus getHealthStatus(HostPort hostPort) {
        RedisHealthCheckInstance key = allHealthStatus.keySet().stream()
                .filter(instance -> instance.getCheckInfo().getHostPort().equals(hostPort))
                .findFirst().orElse(null);

        return null == key ? null : allHealthStatus.get(key);
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

    protected class CollectorPsubActionListener implements PsubActionListener {

        @Override
        public void onAction(PsubActionContext psubActionContext) {
            HealthStatus healthStatus = createOrGetHealthStatus(psubActionContext.instance());
            if (!psubActionContext.getResult().isEmpty()) {
                healthStatus.subSuccess();
            }
        }

        @Override
        public void stopWatch(HealthCheckAction<RedisHealthCheckInstance> action) {
            removeHealthStatus(action);
        }
    }
}
