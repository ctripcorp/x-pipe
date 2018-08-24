package com.ctrip.xpipe.redis.console.healthcheck.impl;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.console.health.RedisSession;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckContext;
import com.ctrip.xpipe.redis.console.healthcheck.HealthStatusManager;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.console.healthcheck.config.HealthCheckConfig;

import java.util.Objects;

/**
 * @author chen.zhu
 * <p>
 * Aug 27, 2018
 */
public class DefaultRedisHealthCheckInstance extends AbstractLifecycle implements RedisHealthCheckInstance {

    private HealthCheckContext healthCheckContext;

    private RedisInstanceInfo redisInstanceInfo;

    private HealthCheckConfig healthCheckConfig;

    private Endpoint endpoint;

    private RedisSession session;

    private HealthStatusManager healthStatusManager;

    public DefaultRedisHealthCheckInstance(HealthCheckContext healthCheckContext, RedisInstanceInfo redisInstanceInfo,
                                           HealthCheckConfig healthCheckConfig, Endpoint endpoint, RedisSession session,
                                           HealthStatusManager healthStatusManager) {
        this.healthCheckContext = healthCheckContext;
        this.redisInstanceInfo = redisInstanceInfo;
        this.healthCheckConfig = healthCheckConfig;
        this.endpoint = endpoint;
        this.session = session;
        this.healthStatusManager = healthStatusManager;
    }

    public DefaultRedisHealthCheckInstance() {}

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

    public DefaultRedisHealthCheckInstance setHealthCheckContext(HealthCheckContext healthCheckContext) {
        this.healthCheckContext = healthCheckContext;
        return this;
    }

    @Override
    public HealthCheckContext getHealthCheckContext() {
        return healthCheckContext;
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
    public HealthStatusManager getHealthStatusManager() {
        return healthStatusManager;
    }

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        LifecycleHelper.initializeIfPossible(getHealthCheckContext());
        LifecycleHelper.initializeIfPossible(getRedisSession());
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        LifecycleHelper.startIfPossible(getHealthCheckContext());
    }

    @Override
    protected void doStop() throws Exception {
        LifecycleHelper.stopIfPossible(getHealthCheckContext());
        super.doStop();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DefaultRedisHealthCheckInstance that = (DefaultRedisHealthCheckInstance) o;
        return Objects.equals(redisInstanceInfo, that.redisInstanceInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(redisInstanceInfo);
    }
}
