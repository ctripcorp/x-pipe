package com.ctrip.xpipe.redis.console.healthcheck.actions.redisstats.tombstonesize;

import com.ctrip.xpipe.redis.console.healthcheck.AbstractActionContext;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;

public class TombstoneSizeActionContext extends AbstractActionContext<Long> {

    public TombstoneSizeActionContext(RedisHealthCheckInstance instance, Long result) {
        super(instance, result);
    }

}
