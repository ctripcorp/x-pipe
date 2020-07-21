package com.ctrip.xpipe.redis.console.healthcheck.actions.redisstats.conflic;

import com.ctrip.xpipe.redis.console.healthcheck.AbstractActionContext;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;

public class CrdtConflictCheckContext extends AbstractActionContext<CrdtConflictStats> {

    public CrdtConflictCheckContext(RedisHealthCheckInstance instance, CrdtConflictStats result) {
        super(instance, result);
    }

}
