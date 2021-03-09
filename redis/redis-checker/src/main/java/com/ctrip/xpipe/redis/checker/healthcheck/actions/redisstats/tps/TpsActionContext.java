package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.tps;

import com.ctrip.xpipe.redis.checker.healthcheck.AbstractActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;

public class TpsActionContext extends AbstractActionContext<Long, RedisHealthCheckInstance> {

    public TpsActionContext(RedisHealthCheckInstance instance, Long result) {
        super(instance, result);
    }

}
