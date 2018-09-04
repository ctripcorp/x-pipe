package com.ctrip.xpipe.redis.console.healthcheck.impl;

import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
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

    private RedisContext redisContext;

    private RedisConfContext redisConfContext;

    private DelayContext delayContext;

    private PingContext pingContext;

    public DefaultHealthCheckContext(RedisContext redisContext,
                                     RedisConfContext redisConfContext,
                                     DelayContext delayContext, PingContext pingContext) {
        this.redisContext = redisContext;
        this.redisConfContext = redisConfContext;
        this.delayContext = delayContext;
        this.pingContext = pingContext;
    }

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        LifecycleHelper.initializeIfPossible(redisConfContext);
        LifecycleHelper.initializeIfPossible(redisConfContext);
        LifecycleHelper.initializeIfPossible(delayContext);
        LifecycleHelper.initializeIfPossible(pingContext);
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        LifecycleHelper.startIfPossible(redisConfContext);
        LifecycleHelper.startIfPossible(redisConfContext);
        LifecycleHelper.startIfPossible(delayContext);
        LifecycleHelper.startIfPossible(pingContext);
    }

    @Override
    protected void doStop() throws Exception {
        LifecycleHelper.stopIfPossible(redisConfContext);
        LifecycleHelper.stopIfPossible(redisConfContext);
        LifecycleHelper.stopIfPossible(delayContext);
        LifecycleHelper.stopIfPossible(pingContext);
        super.doStop();
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

}
