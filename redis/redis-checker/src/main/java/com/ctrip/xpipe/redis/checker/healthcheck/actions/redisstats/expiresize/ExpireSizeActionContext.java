package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.expiresize;

import com.ctrip.xpipe.redis.checker.healthcheck.AbstractActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;

public class ExpireSizeActionContext extends AbstractActionContext<Long, RedisHealthCheckInstance> {

    public ExpireSizeActionContext(RedisHealthCheckInstance instance, Long result) {
        super(instance, result);
    }

}
