package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.conflic;

import com.ctrip.xpipe.redis.checker.healthcheck.AbstractActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;

public class CrdtConflictCheckContext extends AbstractActionContext<CrdtConflictStats, RedisHealthCheckInstance> {

    public CrdtConflictCheckContext(RedisHealthCheckInstance instance, CrdtConflictStats result) {
        super(instance, result);
    }

}
