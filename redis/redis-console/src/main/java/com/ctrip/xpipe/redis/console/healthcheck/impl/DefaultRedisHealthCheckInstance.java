package com.ctrip.xpipe.redis.console.healthcheck.impl;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.console.health.RedisSession;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.console.healthcheck.config.HealthCheckConfig;
import com.ctrip.xpipe.utils.ObjectUtils;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Aug 27, 2018
 */
public class DefaultRedisHealthCheckInstance extends AbstractLifecycle implements RedisHealthCheckInstance {

    private List<HealthCheckAction> actions = Lists.newArrayList();

    private RedisInstanceInfo redisInstanceInfo;

    private HealthCheckConfig healthCheckConfig;

    private Endpoint endpoint;

    private RedisSession session;

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
        return String.format("DefaultRedisHealthCheckInstance{endpoint=%s}", endpoint);
    }
}
