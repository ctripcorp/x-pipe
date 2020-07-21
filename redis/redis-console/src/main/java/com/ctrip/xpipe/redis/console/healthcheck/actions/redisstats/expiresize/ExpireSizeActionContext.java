package com.ctrip.xpipe.redis.console.healthcheck.actions.redisstats.expiresize;

import com.ctrip.xpipe.redis.console.healthcheck.AbstractActionContext;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;

public class ExpireSizeActionContext extends AbstractActionContext<Long> {

    public ExpireSizeActionContext(RedisHealthCheckInstance instance, Long result) {
        super(instance, result);
    }

}
