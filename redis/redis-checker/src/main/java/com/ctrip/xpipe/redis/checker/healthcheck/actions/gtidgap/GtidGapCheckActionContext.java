package com.ctrip.xpipe.redis.checker.healthcheck.actions.gtidgap;

import com.ctrip.xpipe.redis.checker.healthcheck.AbstractActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;

public class GtidGapCheckActionContext extends AbstractActionContext<Long, RedisHealthCheckInstance> {

    public GtidGapCheckActionContext(RedisHealthCheckInstance instance, long info) {
        super(instance, info);
    }

    public GtidGapCheckActionContext(RedisHealthCheckInstance instance, Throwable t) {
        super(instance, t);
    }

}
