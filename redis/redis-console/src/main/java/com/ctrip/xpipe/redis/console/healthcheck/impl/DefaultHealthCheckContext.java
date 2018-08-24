package com.ctrip.xpipe.redis.console.healthcheck.impl;

import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckContext;
import com.ctrip.xpipe.redis.console.healthcheck.HealthStatusManager;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.delay.DelayContext;
import com.ctrip.xpipe.redis.console.healthcheck.ping.PingContext;
import com.ctrip.xpipe.redis.console.healthcheck.redis.RedisContext;
import com.ctrip.xpipe.redis.console.healthcheck.redis.conf.RedisConfContext;

/**
 * @author chen.zhu
 * <p>
 * Aug 27, 2018
 */
public class DefaultHealthCheckContext extends AbstractLifecycle implements HealthCheckContext {

    private RedisHealthCheckInstance instance;

    private RedisContext redisContext;

    private RedisConfContext redisConfContext;

    private DelayContext delayContext;

    private PingContext pingContext;

    private HealthStatusManager.MarkDownReason reason;

    public DefaultHealthCheckContext(RedisHealthCheckInstance instance, RedisContext redisContext,
                                     RedisConfContext redisConfContext,
                                     DelayContext delayContext, PingContext pingContext) {
        this.instance = instance;
        this.redisContext = redisContext;
        this.redisConfContext = redisConfContext;
        this.delayContext = delayContext;
        this.pingContext = pingContext;
    }

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();

    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        redisContext.start();
        redisConfContext.start();
        delayContext.start();
        pingContext.start();
    }

    @Override
    protected void doStop() throws Exception {
        redisContext.stop();
        redisConfContext.stop();
        delayContext.stop();
        pingContext.stop();
        super.doStop();
    }

    @Override
    protected void doDispose() throws Exception {
        super.doDispose();
    }

    @Override
    public RedisContext getRedisContext() {
        return redisContext;
    }

    @Override
    public DelayContext getDelayContext() {
        return delayContext;
    }

    @Override
    public PingContext getPingContext() {
        return pingContext;
    }

    @Override
    public RedisConfContext getRedisConfContext() {
        return redisConfContext;
    }

    @Override
    public HealthStatusManager.MarkDownReason lastMarkDownReason() {
        return reason;
    }
}
