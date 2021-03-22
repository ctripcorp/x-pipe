package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.tombstonesize;

import com.ctrip.xpipe.redis.checker.healthcheck.AbstractActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;

public class TombstoneSizeActionContext extends AbstractActionContext<Long, RedisHealthCheckInstance> {

    public TombstoneSizeActionContext(RedisHealthCheckInstance instance, Long result) {
        super(instance, result);
    }

}
