package com.ctrip.xpipe.redis.console.healthcheck.impl;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.console.healthcheck.*;
import com.ctrip.xpipe.redis.console.healthcheck.actions.delay.DelayActionContext;
import com.ctrip.xpipe.redis.console.healthcheck.actions.ping.PingActionContext;
import com.ctrip.xpipe.redis.console.healthcheck.config.HealthCheckConfig;
import com.ctrip.xpipe.redis.console.healthcheck.session.RedisSession;
import com.ctrip.xpipe.utils.ObjectUtils;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Aug 27, 2018
 */
public class DefaultRedisHealthCheckInstance extends AbstractLifecycle implements RedisHealthCheckInstance {

    private List<HealthCheckAction> actions = Lists.newCopyOnWriteArrayList();

    private RedisInstanceInfo redisInstanceInfo;

    private HealthCheckConfig healthCheckConfig;

    private Endpoint endpoint;

    private RedisSession session;

    private volatile long lastPongTime = -1, lastDelayTime = -1, lastDelayNano = -1;

    public DefaultRedisHealthCheckInstance setRedisInstanceInfo(RedisInstanceInfo redisInstanceInfo) {
        this.redisInstanceInfo = redisInstanceInfo;
        return this;
    }

    public DefaultRedisHealthCheckInstance setHealthCheckConfig(HealthCheckConfig healthCheckConfig) {
        this.healthCheckConfig = healthCheckConfig;
        return this;
    }

    public DefaultRedisHealthCheckInstance setEndpoint(Endpoint endpoint) {
        this.endpoint = endpoint;
        return this;
    }

    public DefaultRedisHealthCheckInstance setSession(RedisSession session) {
        this.session = session;
        return this;
    }

    @Override
    public RedisInstanceInfo getRedisInstanceInfo() {
        return redisInstanceInfo;
    }

    @Override
    public HealthCheckConfig getHealthCheckConfig() {
        return healthCheckConfig;
    }

    @Override
    public Endpoint getEndpoint() {
        return endpoint;
    }

    @Override
    public RedisSession getRedisSession() {
        return session;
    }

    @Override
    public void register(HealthCheckAction action) {
        actions.add(action);
    }

    @Override
    public void unregister(HealthCheckAction action) {
        actions.remove(action);
    }

    @Override
    public List<HealthCheckAction> getHealthCheckActions() {
        return actions;
    }

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        for(HealthCheckAction action : actions) {
            LifecycleHelper.initializeIfPossible(action);
        }
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        for(HealthCheckAction action : actions) {
            LifecycleHelper.startIfPossible(action);
        }
    }

    @Override
    protected void doStop() throws Exception {
       for(HealthCheckAction action : actions) {
           try {
               LifecycleHelper.stopIfPossible(action);
           } catch (Exception e) {
               logger.error("[stop] {}", this.toString(), e);
           }
       }
       actions.clear();
       super.doStop();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DefaultRedisHealthCheckInstance that = (DefaultRedisHealthCheckInstance) o;
        return ObjectUtils.equals(that.getRedisInstanceInfo().getHostPort(),
                this.getRedisInstanceInfo().getHostPort());
    }

    @Override
    public int hashCode() {
        return getRedisInstanceInfo().getHostPort().hashCode();
    }

    @Override
    public String toString() {
        return String.format("HealthCheckInstance[lastPongTime=%d, lastDelayTime=%d, lastDelayNano=%d], InstanceInfo: [%s]",
                lastPongTime, lastDelayTime, lastDelayNano, getRedisInstanceInfo().toString());
    }

    public HealthCheckActionListener createPingListener() {
        return new PingPresentListener();
    }

    public HealthCheckActionListener createDelayListener() {
        return new DelayPresentListener();
    }


    private class PingPresentListener implements HealthCheckActionListener<PingActionContext> {

        @Override
        public void onAction(PingActionContext context) {
            if(context.getResult()) {
                lastPongTime = context.getRecvTimeMilli();
            }
        }

        @Override
        public boolean worksfor(ActionContext t) {
            return t instanceof PingActionContext;
        }

        @Override
        public void stopWatch(HealthCheckAction action) {

        }
    }

    private class DelayPresentListener implements HealthCheckActionListener<DelayActionContext> {

        @Override
        public void onAction(DelayActionContext context) {
            lastDelayTime = context.getRecvTimeMilli();
            lastDelayNano = context.getResult();
        }

        @Override
        public boolean worksfor(ActionContext t) {
            return t instanceof DelayActionContext;
        }

        @Override
        public void stopWatch(HealthCheckAction action) {

        }
    }
}
