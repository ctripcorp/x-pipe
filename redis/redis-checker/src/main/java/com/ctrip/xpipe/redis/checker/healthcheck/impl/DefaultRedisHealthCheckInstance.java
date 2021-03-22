package com.ctrip.xpipe.redis.checker.healthcheck.impl;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.redis.checker.healthcheck.*;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.delay.DelayActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.ping.PingActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import com.ctrip.xpipe.utils.ObjectUtils;

/**
 * @author chen.zhu
 * <p>
 * Aug 27, 2018
 */
public class DefaultRedisHealthCheckInstance extends AbstractHealthCheckInstance<RedisInstanceInfo> implements RedisHealthCheckInstance {

    private Endpoint endpoint;

    private RedisSession session;

    private volatile long lastPongTime = -1, lastDelayTime = -1, lastDelayNano = -1;

    public DefaultRedisHealthCheckInstance setEndpoint(Endpoint endpoint) {
        this.endpoint = endpoint;
        return this;
    }

    public DefaultRedisHealthCheckInstance setSession(RedisSession session) {
        this.session = session;
        return this;
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DefaultRedisHealthCheckInstance that = (DefaultRedisHealthCheckInstance) o;
        return ObjectUtils.equals(that.getCheckInfo().getHostPort(),
                this.getCheckInfo().getHostPort());
    }

    @Override
    public int hashCode() {
        return getCheckInfo().getHostPort().hashCode();
    }

    @Override
    public String toString() {
        return String.format("HealthCheckInstance[lastPongTime=%d, lastDelayTime=%d, lastDelayNano=%d], InstanceInfo: [%s]",
                lastPongTime, lastDelayTime, lastDelayNano, getCheckInfo().toString());
    }

    public HealthCheckActionListener createPingListener() {
        return new PingPresentListener();
    }

    public HealthCheckActionListener createDelayListener() {
        return new DelayPresentListener();
    }


    private class PingPresentListener implements HealthCheckActionListener<PingActionContext, HealthCheckAction> {

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

    private class DelayPresentListener implements HealthCheckActionListener<DelayActionContext, HealthCheckAction> {

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
