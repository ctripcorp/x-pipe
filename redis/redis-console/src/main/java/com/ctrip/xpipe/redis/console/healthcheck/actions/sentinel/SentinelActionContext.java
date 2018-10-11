package com.ctrip.xpipe.redis.console.healthcheck.actions.sentinel;

import com.ctrip.xpipe.redis.console.healthcheck.AbstractActionContext;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;

import java.util.Set;

/**
 * @author chen.zhu
 * <p>
 * Oct 09, 2018
 */
public class SentinelActionContext extends AbstractActionContext<Set<SentinelHello>> {

    public SentinelActionContext(RedisHealthCheckInstance instance, Set<SentinelHello> sentinelHellos) {
        super(instance, sentinelHellos);
    }
}
