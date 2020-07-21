package com.ctrip.xpipe.redis.console.healthcheck.actions.redisstats.tps;

import com.ctrip.xpipe.redis.console.healthcheck.AbstractActionContext;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;

public class TpsActionContext extends AbstractActionContext<Long> {

    public TpsActionContext(RedisHealthCheckInstance instance, Long result) {
        super(instance, result);
    }

}
