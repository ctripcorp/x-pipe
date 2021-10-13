package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel;

import com.ctrip.xpipe.redis.checker.healthcheck.AbstractActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;

import java.util.Set;

/**
 * @author chen.zhu
 * <p>
 * Oct 09, 2018
 */
public class SentinelActionContext extends AbstractActionContext<Set<SentinelHello>, RedisHealthCheckInstance> {

    public SentinelActionContext(RedisHealthCheckInstance instance, Throwable t) {
        super(instance, t);
    }

    public SentinelActionContext(RedisHealthCheckInstance instance, Set<SentinelHello> sentinelHellos) {
        super(instance, sentinelHellos);
    }

    @Override
    public String toString() {
        return "SentinelActionContext{" +
                "c=" + c +
                ", instance=" + instance +
                ", cause=" + cause +
                '}';
    }
}
